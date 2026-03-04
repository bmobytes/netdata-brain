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
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import requests
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

# ── SNMP node / switch config ──────────────────────────────────────────────────
SNMP_NODE_IP = os.environ.get("SNMP_NODE_IP", "192.168.140.134")

# ── UniFi config ───────────────────────────────────────────────────────────────
UNIFI_URL     = os.environ.get("UNIFI_URL", "https://192.168.140.1")
UNIFI_API_KEY = os.environ.get("UNIFI_API", "")
UNIFI_SITE    = os.environ.get("UNIFI_SITE", "default")

# ── TrueNAS config ─────────────────────────────────────────────────────────────
TRUENAS_URL     = os.environ.get("TRUENAS_URL", "")      # optional; skip all TrueNAS if not set
TRUENAS_API_KEY = os.environ.get("TRUENAS_API_KEY", "")

# Prometheus-via-Netdata switch sources on the SNMP node.
# key = device name (as it appears in prometheus_ chart prefix), value = human label.
# NOTE: C3569 and CR-01 went stale 2026-02-05 — collector stopped scraping them.
# CSR-SAN is active; msn-ls-csr-san-01 is actually a MikroTik router (ether1/bridge1 ifaces).
SWITCH_SOURCES = {
    "msn-ls-csr-san-01": "CSR-SAN-01",
}

# Metrics to pull per port (metric_name → InfluxDB field name suffix)
SWITCH_METRICS = {
    "ifHCInOctets":  "bytes_in",
    "ifHCOutOctets": "bytes_out",
    "ifOperStatus":  "oper_status",
    "ifInErrors":    "errors_in",
    "ifOutErrors":   "errors_out",
}

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

    def get_chart_data_by_url(self, data_url: str, points: int = 1) -> Optional[dict]:
        """
        Fetch chart data using Netdata's pre-built data_url (from /api/v1/charts).
        The data_url already contains the correctly-encoded chart name, avoiding
        URL encoding issues with special chars (=, ", /) in prometheus chart IDs.
        """
        try:
            base_host = self.base.replace("/api/v1", "")
            url = (f"{base_host}{data_url}"
                   f"&after={-(POLL_INTERVAL + 5)}&before=0&points={points}&format=json&group=average")
            resp = self.session.get(url, timeout=self.timeout)
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


# ── Switch port collection ─────────────────────────────────────────────────────
import re as _re

def _parse_port_info(chart_name: str) -> dict:
    """
    Parse port metadata from a Netdata prometheus chart name like:
      prometheus_msn-ls-c3569-as-01.ifHCInOctets-ifAlias="AP_Uplinks"-ifDescr=GigabitEthernet1/0/43-ifIndex=50-ifName=Gi1/0/43
    Returns dict with keys: ifAlias, ifDescr, ifIndex, ifName
    """
    alias_m = _re.search(r'ifAlias="([^"]*)"', chart_name)
    descr_m = _re.search(r'ifDescr=([^-]+)', chart_name)
    idx_m   = _re.search(r'ifIndex=(\d+)', chart_name)
    name_m  = _re.search(r'ifName=(\S+)$', chart_name)
    return {
        "ifAlias": alias_m.group(1) if alias_m else "",
        "ifDescr": descr_m.group(1) if descr_m else "",
        "ifIndex": int(idx_m.group(1)) if idx_m else 0,
        "ifName":  name_m.group(1) if name_m else "",
    }


def discover_switch_port_charts(snmp_client: AgentClient, device: str) -> list[dict]:
    """
    Discover all port charts for a device using /api/v1/charts metadata.
    Stores data_url (pre-sanitized by Netdata) to avoid URL encoding issues
    with prometheus chart IDs that contain =, ", and / characters.
    Returns list of dicts: [{index, name, alias, descr, charts: {metric: data_url}}]
    """
    try:
        base_host = snmp_client.base.replace("/api/v1", "")
        resp = snmp_client.session.get(f"{snmp_client.base}/charts", timeout=10)
        resp.raise_for_status()
        all_charts = resp.json().get("charts", {})
    except Exception as e:
        logger.error(f"Failed to fetch charts for switch discovery: {e}")
        return []

    prefix = f"prometheus_{device}."
    port_map: dict[int, dict] = {}

    for chart_id, chart_obj in all_charts.items():
        if not chart_id.startswith(prefix):
            continue
        metric_part = chart_id[len(prefix):]
        # Extract base metric name (before any -ifAlias / -ifDescr suffix)
        base_metric = metric_part.split("-ifAlias")[0].split("-ifDescr")[0]
        if base_metric not in SWITCH_METRICS:
            continue
        data_url = chart_obj.get("data_url", "")
        if not data_url:
            continue
        # Skip stale charts (no data in last 2 hours)
        import time as _time
        last_entry = chart_obj.get("last_entry", 0)
        if _time.time() - last_entry > 7200:
            continue

        info = _parse_port_info(chart_id)
        idx = info["ifIndex"]
        if idx == 0:
            continue
        if idx not in port_map:
            port_map[idx] = {
                "index": idx,
                "name":  info["ifName"],
                "alias": info["ifAlias"],
                "descr": info["ifDescr"],
                "charts": {},
            }
        port_map[idx]["charts"][base_metric] = data_url

    return list(port_map.values())


def collect_switch_port(snmp_client: AgentClient, port: dict, device_label: str,
                        ts: datetime) -> list[Point]:
    """Collect all available metrics for one switch port and return InfluxDB Points."""
    charts = port.get("charts", {})
    if not charts:
        return []

    # Require at minimum the traffic charts
    if "ifHCInOctets" not in charts and "ifHCOutOctets" not in charts:
        return []

    p = (
        Point("switch_port")
        .tag("device",     device_label)
        .tag("port_name",  port["name"])
        .tag("port_alias", port["alias"] or port["name"])
        .tag("port_index", str(port["index"]))
        .time(ts, "s")
    )

    has_data = False
    for metric, field_suffix in SWITCH_METRICS.items():
        data_url = charts.get(metric)
        if not data_url:
            continue
        data = snmp_client.get_chart_data_by_url(data_url, points=1)
        vals = extract_latest(data) if data else None
        if not vals:
            continue
        # Most switch metric charts have a single dimension named after the metric
        # or standard names like 'value', 'running', etc.
        value = None
        if metric == "ifOperStatus":
            # Dimension 'running' = 1 means up; sum positive dims
            value = float(1 if any(v and v > 0 for v in vals.values()) else 0)
        else:
            # Take first non-zero positive value or first value
            for v in vals.values():
                if v is not None:
                    value = float(abs(v))
                    break
        if value is not None:
            p = p.field(field_suffix, value)
            has_data = True

    return [p] if has_data else []


def collect_switch_device(device: str, device_label: str,
                          snmp_client: AgentClient, port_cache: dict,
                          ts: datetime) -> tuple[str, list[Point]]:
    """
    Collect all port metrics for one switch device.
    port_cache[device] is refreshed from discover_switch_port_charts() when stale.
    Returns (device_label, list_of_points).
    """
    ports = port_cache.get(device)
    if not ports:
        logger.debug(f"Switch {device}: no port cache, skipping")
        return device_label, []

    all_points = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(collect_switch_port, snmp_client, port, device_label, ts): port
            for port in ports
        }
        for fut in as_completed(futures):
            all_points.extend(fut.result())

    logger.debug(f"Switch {device_label}: {len(all_points)} port data points")
    return device_label, all_points


# ── Alarm collection ───────────────────────────────────────────────────────────
def collect_alarms(nodes: list[dict]) -> list[Point]:
    """
    Poll each reachable node's local alarm API and write active alarms to InfluxDB.
    Measurement: node_alarms
    Tags: node_name, alarm_name
    Fields: severity (1=warn, 2=crit), silenced (0/1), summary (string), value (float)
    """
    all_points = []
    ts = datetime.now(timezone.utc)

    for node in nodes:
        ip   = node["ip"]
        name = node["name"]
        try:
            resp = requests.get(
                f"http://{ip}:{AGENT_PORT}/api/v1/alarms",
                params={"active": "true"},
                timeout=AGENT_TIMEOUT,
            )
            if resp.status_code != 200:
                continue
            data   = resp.json()
            alarms = data.get("alarms", {})
            for alarm_key, alarm in alarms.items():
                status = alarm.get("status", "")
                if status not in ("WARNING", "CRITICAL"):
                    continue
                severity = 2 if status == "CRITICAL" else 1
                summary  = alarm.get("summary") or alarm.get("info") or alarm.get("name", alarm_key)
                p = (
                    Point("node_alarms")
                    .tag("node_name",  name)
                    .tag("alarm_name", alarm.get("name", alarm_key))
                    .field("severity", severity)
                    .field("silenced", int(bool(alarm.get("silenced", False))))
                    .field("summary",  summary[:200])          # cap string field length
                    .field("value",    float(alarm.get("value") or 0))
                    .time(ts, "s")
                )
                all_points.append(p)
        except Exception as e:
            logger.debug(f"Alarm fetch failed for {name} ({ip}): {e}")

    logger.debug(f"Alarms: collected {len(all_points)} active alarm points")
    return all_points


# ── UniFi device collection ────────────────────────────────────────────────────
def collect_unifi_devices() -> list[Point]:
    """
    Poll the UniFi controller for device status and write to InfluxDB.
    Measurement: unifi_device
    Tags: mac, name, model, device_type
    Fields: state (0/1), uptime (seconds), clients (int)
    """
    if not UNIFI_API_KEY:
        return []

    ts = datetime.now(timezone.utc)
    try:
        resp = requests.get(
            f"{UNIFI_URL}/proxy/network/api/s/{UNIFI_SITE}/stat/device",
            headers={"X-API-KEY": UNIFI_API_KEY},
            verify=False,
            timeout=10,
        )
        resp.raise_for_status()
        devices = resp.json().get("data", [])

        points = []
        for dev in devices:
            p = (
                Point("unifi_device")
                .tag("mac",         dev.get("mac", ""))
                .tag("name",        dev.get("name", dev.get("mac", "")))
                .tag("model",       dev.get("model", ""))
                .tag("device_type", dev.get("type", ""))
                .field("state",   int(dev.get("state",   0)))
                .field("uptime",  int(dev.get("uptime",  0)))
                .field("clients", int(dev.get("num_sta", 0)))
                .time(ts, "s")
            )
            points.append(p)

        logger.debug(f"UniFi: collected {len(points)} device points")
        return points
    except Exception as e:
        logger.error(f"UniFi device collection failed: {e}")
        return []


# ── TrueNAS collection ────────────────────────────────────────────────────────
def _truenas_get(path: str, **kwargs) -> Optional[dict]:
    """GET helper for TrueNAS API."""
    if not TRUENAS_URL:
        return None
    try:
        resp = requests.get(
            f"{TRUENAS_URL}{path}",
            headers={"Authorization": f"Bearer {TRUENAS_API_KEY}"},
            timeout=10,
            **kwargs,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"TrueNAS GET {path} failed: {e}")
        return None


def _truenas_post(path: str, payload: dict) -> Optional[dict]:
    """POST helper for TrueNAS API."""
    if not TRUENAS_URL:
        return None
    try:
        resp = requests.post(
            f"{TRUENAS_URL}{path}",
            headers={
                "Authorization": f"Bearer {TRUENAS_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"TrueNAS POST {path} failed: {e}")
        return None


def collect_truenas_pools() -> list[Point]:
    """
    Poll TrueNAS pool list and write capacity metrics to InfluxDB.
    Measurement: truenas_pool
    Tags: pool
    Fields: status (string), size_bytes, free_bytes, used_bytes, used_pct (float)
    """
    if not TRUENAS_URL:
        return []
    data = _truenas_get("/api/v2.0/pool")
    if not data:
        return []

    ts = datetime.now(timezone.utc)
    points = []
    for pool in data:
        name = pool.get("name", "unknown")
        topology = pool.get("topology", {})
        size_bytes = pool.get("size", 0) or 0
        free_bytes = pool.get("free", 0) or 0
        used_bytes = size_bytes - free_bytes
        used_pct = (used_bytes / size_bytes * 100) if size_bytes > 0 else 0.0
        p = (
            Point("truenas_pool")
            .tag("pool", name)
            .field("status",     pool.get("status", "UNKNOWN"))
            .field("size_bytes", float(size_bytes))
            .field("free_bytes", float(free_bytes))
            .field("used_bytes", float(used_bytes))
            .field("used_pct",   float(used_pct))
            .time(ts, "s")
        )
        points.append(p)

    logger.debug(f"TrueNAS pools: {len(points)} points")
    return points


def collect_truenas_datasets() -> list[Point]:
    """
    Poll TrueNAS dataset list and write usage metrics to InfluxDB.
    Measurement: truenas_dataset
    Tags: dataset, pool
    Fields: used_bytes, available_bytes, used_pct (float)
    Skips: depth > 2, used_bytes == 0
    """
    if not TRUENAS_URL:
        return []
    data = _truenas_get("/api/v2.0/pool/dataset", params={"limit": 200})
    if not data:
        return []

    ts = datetime.now(timezone.utc)
    points = []
    for ds in data:
        ds_name = ds.get("name", "")
        pool_name = ds_name.split("/")[0] if "/" in ds_name else ds_name
        # Depth = number of slashes
        depth = ds_name.count("/")
        if depth > 2:
            continue
        used_raw  = ds.get("used", {})
        avail_raw = ds.get("available", {})
        used_bytes  = used_raw.get("parsed", 0) if isinstance(used_raw, dict) else (used_raw or 0)
        avail_bytes = avail_raw.get("parsed", 0) if isinstance(avail_raw, dict) else (avail_raw or 0)
        if used_bytes == 0:
            continue
        total = used_bytes + avail_bytes
        used_pct = (used_bytes / total * 100) if total > 0 else 0.0
        p = (
            Point("truenas_dataset")
            .tag("dataset", ds_name)
            .tag("pool",    pool_name)
            .field("used_bytes",      float(used_bytes))
            .field("available_bytes", float(avail_bytes))
            .field("used_pct",        float(used_pct))
            .time(ts, "s")
        )
        points.append(p)

    logger.debug(f"TrueNAS datasets: {len(points)} points")
    return points


def collect_truenas_disk_temps() -> list[Point]:
    """
    Poll TrueNAS disk temperatures and write to InfluxDB.
    Measurement: truenas_disk_temp
    Tags: disk
    Fields: temp_celsius (float)
    Skips: disks where temp is null
    """
    if not TRUENAS_URL:
        return []
    data = _truenas_post("/api/v2.0/disk/temperatures", {})
    if not data:
        return []

    ts = datetime.now(timezone.utc)
    points = []
    # Response: {"sda": 35, "sdb": null, ...}
    for disk, temp in data.items():
        if temp is None:
            continue
        p = (
            Point("truenas_disk_temp")
            .tag("disk", disk)
            .field("temp_celsius", float(temp))
            .time(ts, "s")
        )
        points.append(p)

    logger.debug(f"TrueNAS disk temps: {len(points)} points")
    return points


def collect_truenas_alerts() -> list[Point]:
    """
    Poll TrueNAS active alerts and write to InfluxDB.
    Measurement: truenas_alert
    Tags: level, alert_uuid
    Fields: message (string), dismissed (bool as int)
    """
    if not TRUENAS_URL:
        return []
    data = _truenas_get("/api/v2.0/alert/list")
    if not data:
        return []

    ts = datetime.now(timezone.utc)
    points = []
    for alert in data:
        level = alert.get("level", "INFO")
        uuid  = alert.get("uuid", "")
        msg   = alert.get("formatted", "") or alert.get("text", "") or ""
        dismissed = int(bool(alert.get("dismissed", False)))
        p = (
            Point("truenas_alert")
            .tag("level",      level)
            .tag("alert_uuid", uuid)
            .field("message",   msg[:500])
            .field("dismissed", dismissed)
            .time(ts, "s")
        )
        points.append(p)

    logger.debug(f"TrueNAS alerts: {len(points)} points")
    return points


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

    # Switch port cache — refreshed every 15 minutes
    port_cache: dict[str, list] = {}
    port_cache_refreshed = 0
    snmp_client = AgentClient(SNMP_NODE_IP)

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

        # Refresh switch port chart cache every 15 minutes
        if time.time() - port_cache_refreshed > 900:
            if snmp_client.is_reachable():
                for device in SWITCH_SOURCES:
                    ports = discover_switch_port_charts(snmp_client, device)
                    if ports:
                        port_cache[device] = ports
                        logger.info(f"Switch {device}: discovered {len(ports)} ports")
                    else:
                        logger.warning(f"Switch {device}: no ports discovered")
                port_cache_refreshed = time.time()
            else:
                logger.warning(f"SNMP node {SNMP_NODE_IP} unreachable, skipping switch discovery")

        nodes = list(node_map.values())
        all_points = []
        ok_count = 0
        ts = datetime.now(timezone.utc)

        # Collect from all nodes in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(collect_node, n): n for n in nodes}
            for future in as_completed(futures):
                name, points = future.result()
                if points:
                    all_points.extend(points)
                    ok_count += 1

        # Collect switch port metrics (sequential per device, parallel per port)
        switch_points = 0
        if port_cache and snmp_client.is_reachable():
            for device, label in SWITCH_SOURCES.items():
                _, pts = collect_switch_device(device, label, snmp_client, port_cache, ts)
                all_points.extend(pts)
                switch_points += len(pts)

        # Collect active alarms from all nodes
        alarm_pts = collect_alarms(nodes)
        all_points.extend(alarm_pts)

        # Collect UniFi device status
        unifi_pts = collect_unifi_devices() if UNIFI_API_KEY else []
        all_points.extend(unifi_pts)

        # Collect TrueNAS metrics (if TRUENAS_URL is configured)
        truenas_pts = []
        if TRUENAS_URL:
            truenas_pts.extend(collect_truenas_pools())
            truenas_pts.extend(collect_truenas_datasets())
            truenas_pts.extend(collect_truenas_disk_temps())
            truenas_pts.extend(collect_truenas_alerts())
            all_points.extend(truenas_pts)

        # Write batch to InfluxDB
        if all_points:
            try:
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=all_points)
                logger.info(
                    f"Wrote {len(all_points)} points: "
                    f"{ok_count}/{len(nodes)} nodes, "
                    f"{switch_points} switch port pts, "
                    f"{len(alarm_pts)} alarm pts, "
                    f"{len(unifi_pts)} unifi pts, "
                    f"{len(truenas_pts)} truenas pts"
                )
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
