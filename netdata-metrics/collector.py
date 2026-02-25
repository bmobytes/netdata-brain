"""
netdata-metrics — detailed metrics collector.

Runs on a host with direct access to Netdata agents (port 19999).
Polls CPU, RAM, network, disk, I/O, load, and containers per node.
Writes to a remote InfluxDB instance.
"""

import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import requests
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("netdata-metrics")

# ── Config ─────────────────────────────────────────────────────────────────────
def require_env(k):
    v = os.environ.get(k)
    if not v:
        logger.error(f"Required env var {k} not set")
        sys.exit(1)
    return v

NETDATA_API      = require_env("NETDATA_API")
NETDATA_SPACE    = require_env("NETDATA_SPACE_ID")
NETDATA_ROOM     = require_env("NETDATA_ROOM_ID")
INFLUXDB_URL     = require_env("INFLUXDB_URL")
INFLUXDB_TOKEN   = require_env("INFLUXDB_TOKEN")
INFLUXDB_ORG     = require_env("INFLUXDB_ORG")
INFLUXDB_BUCKET  = require_env("INFLUXDB_BUCKET")
POLL_INTERVAL    = int(os.environ.get("POLL_INTERVAL", "60"))
AGENT_PORT       = int(os.environ.get("AGENT_PORT", "19999"))
AGENT_TIMEOUT    = int(os.environ.get("AGENT_TIMEOUT", "5"))
MAX_WORKERS      = int(os.environ.get("MAX_WORKERS", "8"))

CLOUD_BASE = "https://app.netdata.cloud/api/v2"

# Charts to collect and their InfluxDB measurement names
CHART_MAP = {
    "system.cpu":    "node_cpu",
    "system.ram":    "node_memory",
    "system.net":    "node_network",
    "system.load":   "node_load",
    "system.io":     "node_disk_io",
    "mem.available": "node_memory",
    "disk_space./":  "node_disk_space",
}

# ── Cloud API: build node→IP map ───────────────────────────────────────────────
def get_node_ip_map() -> dict[str, dict]:
    """Returns {node_id: {name, ip, os, os_name}} for all nodes with known IPs."""
    url = f"{CLOUD_BASE}/spaces/{NETDATA_SPACE}/rooms/{NETDATA_ROOM}/nodes"
    headers = {"Authorization": f"Bearer {NETDATA_API}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        nodes = resp.json()
        result = {}
        for n in nodes:
            labels = n.get("hostLabels") or {}
            ip = labels.get("_net_default_iface_ip", "")
            vtype = labels.get("_vnode_type", "")
            if not ip or vtype:  # skip SNMP virtual nodes and nodes without IPs
                continue
            if n.get("state") != "reachable":
                continue
            result[n["id"]] = {
                "name": n["name"],
                "ip": ip,
                "os": n.get("os", ""),
                "os_name": n.get("osName", ""),
                "node_id": n["id"],
            }
        logger.info(f"Got {len(result)} queryable nodes from Cloud API")
        return result
    except Exception as e:
        logger.error(f"Failed to get node map: {e}")
        return {}


# ── Agent API ──────────────────────────────────────────────────────────────────
class AgentClient:
    def __init__(self, ip: str, port: int = AGENT_PORT, timeout: int = AGENT_TIMEOUT):
        self.base = f"http://{ip}:{port}/api/v1"
        self.timeout = timeout
        self.session = requests.Session()

    def get_chart_data(self, chart: str, points: int = 1) -> Optional[dict]:
        """Fetch the latest data point for a chart."""
        try:
            url = f"{self.base}/data"
            params = {
                "chart": chart,
                "after": -(POLL_INTERVAL + 5),
                "before": 0,
                "points": points,
                "format": "json",
                "group": "average",
            }
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.ConnectionError:
            return None
        except Exception:
            return None

    def get_charts_list(self) -> list[str]:
        """Get list of all chart IDs on this agent."""
        try:
            resp = self.session.get(f"{self.base}/charts", timeout=self.timeout)
            resp.raise_for_status()
            return list(resp.json().get("charts", {}).keys())
        except Exception:
            return []

    def is_reachable(self) -> bool:
        try:
            resp = self.session.get(f"{self.base}/info", timeout=2)
            return resp.status_code == 200
        except Exception:
            return False


# ── Metric extraction ──────────────────────────────────────────────────────────
def extract_latest(data: dict) -> Optional[dict[str, float]]:
    """Extract the most recent data row as {dimension: value}."""
    if not data:
        return None
    labels = data.get("labels", [])
    rows = data.get("data", [])
    if not rows or not labels:
        return None
    # Labels: ['time', 'dim1', 'dim2', ...]
    # Rows: [[timestamp, val1, val2, ...]]
    row = rows[-1]
    return {labels[i]: row[i] for i in range(1, len(labels)) if i < len(row)}


def build_points(node: dict, charts_data: dict, ts: datetime) -> list[Point]:
    """Convert raw chart data into InfluxDB Points."""
    points = []
    name = node["name"]
    os_name = node["os_name"] or node["os"]

    def base_point(measurement: str) -> Point:
        return (
            Point(measurement)
            .tag("node_name", name)
            .tag("node_id", node["node_id"])
            .tag("os", os_name)
            .time(ts, "s")
        )

    # ── CPU ────────────────────────────────────────────────────────────────────
    if "system.cpu" in charts_data:
        vals = charts_data["system.cpu"]
        if vals:
            used = sum(v for k, v in vals.items()
                       if k not in ("idle", "guest", "guest_nice") and v is not None)
            p = base_point("node_cpu")
            for dim, val in vals.items():
                if val is not None:
                    p = p.field(f"cpu_{dim}_pct", float(val))
            p = p.field("cpu_used_pct", float(used))
            points.append(p)

    # ── Memory ─────────────────────────────────────────────────────────────────
    if "system.ram" in charts_data:
        vals = charts_data["system.ram"]
        if vals:
            p = base_point("node_memory")
            total = sum(v for v in vals.values() if v is not None)
            used = vals.get("used", 0) or 0
            for dim, val in vals.items():
                if val is not None:
                    p = p.field(f"ram_{dim}_mb", float(val))
            if total > 0:
                p = p.field("ram_used_pct", float(used / total * 100))
            p = p.field("ram_total_mb", float(total))
            points.append(p)

    if "mem.available" in charts_data:
        vals = charts_data["mem.available"]
        if vals and "avail" in vals:
            p = base_point("node_memory")
            p = p.field("ram_available_mb", float(vals["avail"]))
            points.append(p)

    # ── Network ────────────────────────────────────────────────────────────────
    if "system.net" in charts_data:
        vals = charts_data["system.net"]
        if vals:
            p = base_point("node_network").tag("interface", "all")
            recv = abs(vals.get("received", 0) or 0)
            sent = abs(vals.get("sent", 0) or 0)
            p = p.field("net_received_kbps", float(recv))
            p = p.field("net_sent_kbps", float(sent))
            points.append(p)

    # Per-interface bandwidth from "net.*" charts
    for chart, vals in charts_data.items():
        if chart.startswith("net.") and chart != "net.lo" and vals:
            iface = chart.split(".", 1)[1]
            p = base_point("node_network_iface").tag("interface", iface)
            recv = abs(vals.get("received", 0) or 0)
            sent = abs(vals.get("sent", 0) or 0)
            p = p.field("received_kbps", float(recv))
            p = p.field("sent_kbps", float(sent))
            points.append(p)

    # ── Load ───────────────────────────────────────────────────────────────────
    if "system.load" in charts_data:
        vals = charts_data["system.load"]
        if vals:
            p = base_point("node_load")
            for dim in ("load1", "load5", "load15"):
                if dim in vals and vals[dim] is not None:
                    p = p.field(dim, float(vals[dim]))
            points.append(p)

    # ── Disk I/O ───────────────────────────────────────────────────────────────
    if "system.io" in charts_data:
        vals = charts_data["system.io"]
        if vals:
            p = base_point("node_disk_io")
            reads = abs(vals.get("reads", 0) or 0)
            writes = abs(vals.get("writes", 0) or 0)
            p = p.field("io_reads_kbs", float(reads))
            p = p.field("io_writes_kbs", float(writes))
            points.append(p)

    # ── Disk Space ─────────────────────────────────────────────────────────────
    for chart, vals in charts_data.items():
        if chart.startswith("disk_space.") and vals:
            mountpoint = chart.replace("disk_space.", "") or "/"
            avail = vals.get("avail", 0) or 0
            used = vals.get("used", 0) or 0
            total = avail + used
            p = base_point("node_disk_space").tag("mountpoint", mountpoint)
            p = p.field("disk_avail_gb", float(avail / 1024))
            p = p.field("disk_used_gb", float(used / 1024))
            if total > 0:
                p = p.field("disk_used_pct", float(used / total * 100))
            points.append(p)

    # ── Containers (cgroups) ───────────────────────────────────────────────────
    for chart, vals in charts_data.items():
        if chart.startswith("cgroup_") and vals:
            # cgroup_<name>.cpu_limit or cgroup_<name>.mem
            parts = chart.split(".")
            if len(parts) < 2:
                continue
            cgroup_name = parts[0].replace("cgroup_", "")
            metric_type = parts[1]

            if metric_type == "cpu_limit" and "used" in vals:
                p = (base_point("container_cpu")
                     .tag("container", cgroup_name)
                     .field("cpu_used_pct", float(vals["used"] or 0)))
                points.append(p)
            elif metric_type == "mem" and "rss" in vals:
                p = (base_point("container_memory")
                     .tag("container", cgroup_name)
                     .field("mem_rss_mb", float(vals.get("rss", 0) or 0))
                     .field("mem_cache_mb", float(vals.get("cache", 0) or 0)))
                points.append(p)

    return points


# ── Per-node collection ────────────────────────────────────────────────────────
def collect_node(node: dict) -> tuple[str, list[Point]]:
    """Collect all metrics for a single node. Returns (node_name, points)."""
    ip = node["ip"]
    name = node["name"]
    client = AgentClient(ip)

    if not client.is_reachable():
        logger.debug(f"  {name} ({ip}): unreachable, skipping")
        return name, []

    # Discover charts once (cached per run)
    all_charts = client.get_charts_list()

    # Build list of charts to query
    charts_to_fetch = set()
    for c in CHART_MAP:
        charts_to_fetch.add(c)
    # Add per-interface net charts
    for c in all_charts:
        if c.startswith("net.") and c != "net.lo":
            charts_to_fetch.add(c)
    # Add disk space charts
    for c in all_charts:
        if c.startswith("disk_space."):
            charts_to_fetch.add(c)
    # Add cgroup charts for containers
    for c in all_charts:
        if c.startswith("cgroup_") and (".cpu_limit" in c or ".mem" in c):
            charts_to_fetch.add(c)

    # Fetch all charts
    charts_data = {}
    for chart in charts_to_fetch:
        data = client.get_chart_data(chart)
        if data:
            vals = extract_latest(data)
            if vals:
                charts_data[chart] = vals

    ts = datetime.now(timezone.utc)
    points = build_points(node, charts_data, ts)
    logger.debug(f"  {name}: {len(charts_data)} charts → {len(points)} points")
    return name, points


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  netdata-metrics starting")
    logger.info(f"  Poll interval: {POLL_INTERVAL}s | Workers: {MAX_WORKERS}")
    logger.info(f"  InfluxDB: {INFLUXDB_URL}")
    logger.info("=" * 60)

    # InfluxDB client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    def health_check():
        try:
            return client.health().status == "pass"
        except Exception:
            return False

    for attempt in range(10):
        if health_check():
            logger.info("InfluxDB connected")
            break
        logger.warning(f"InfluxDB not ready (attempt {attempt+1}/10)")
        time.sleep(5)
    else:
        logger.error("Cannot connect to InfluxDB")
        sys.exit(1)

    # Refresh node map every 10 minutes
    node_map = {}
    node_map_refreshed = 0

    running = True
    def shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal")
        running = False
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while running:
        loop_start = time.time()

        # Refresh node→IP map
        if time.time() - node_map_refreshed > 600:
            node_map = get_node_ip_map()
            node_map_refreshed = time.time()

        nodes = list(node_map.values())
        all_points = []
        ok_count = 0

        # Collect from all nodes in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(collect_node, n): n for n in nodes}
            for future in as_completed(futures):
                name, points = future.result()
                if points:
                    all_points.extend(points)
                    ok_count += 1

        # Write batch to InfluxDB
        if all_points:
            try:
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=all_points)
                logger.info(f"Wrote {len(all_points)} points from {ok_count}/{len(nodes)} nodes")
            except Exception as e:
                logger.error(f"InfluxDB write failed: {e}")
        else:
            logger.warning("No points collected this cycle")

        # Sleep remainder of interval
        elapsed = time.time() - loop_start
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        if running:
            time.sleep(sleep_time)

    logger.info("netdata-metrics shutting down")
    client.close()


if __name__ == "__main__":
    main()
