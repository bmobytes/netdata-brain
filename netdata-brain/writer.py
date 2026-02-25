"""
InfluxDB 2.x writer.
Writes node status metrics as time-series data.
"""

import logging
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from collector import NodeInfo

logger = logging.getLogger(__name__)


def _parse_memory_bytes(mem_str: str) -> int:
    """Parse '7.5 GiB' → bytes."""
    if not mem_str or mem_str in ("0 B", "unknown"):
        return 0
    try:
        parts = mem_str.strip().split()
        value = float(parts[0])
        unit = parts[1].upper() if len(parts) > 1 else "B"
        multipliers = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}
        return int(value * multipliers.get(unit, 1))
    except Exception:
        return 0


class InfluxWriter:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.bucket = bucket
        self.org = org
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        logger.info(f"InfluxDB writer initialized: {url} → {org}/{bucket}")

    def write_nodes(self, nodes: list[NodeInfo]):
        """Write node status snapshot to InfluxDB."""
        points = []
        now = datetime.now(timezone.utc)

        for node in nodes:
            p = (
                Point("node_status")
                .tag("node_name", node.name)
                .tag("node_id", node.id)
                .tag("os", node.os_name or node.os)
                .tag("node_type", node.node_type)
                .tag("state", node.state)
                .field("is_reachable", 1 if node.is_reachable else 0)
                .field("warning_count", node.warning_count)
                .field("critical_count", node.critical_count)
                .field("cpu_count", node.cpus)
                .field("memory_bytes", _parse_memory_bytes(node.memory))
                .time(now, "s")
            )
            points.append(p)

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
            logger.debug(f"Wrote {len(points)} node points to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")

    def write_fleet_summary(self, nodes: list[NodeInfo]):
        """Write fleet-level aggregate metrics."""
        now = datetime.now(timezone.utc)
        reachable = sum(1 for n in nodes if n.is_reachable)
        stale = sum(1 for n in nodes if not n.is_reachable)
        total_critical = sum(n.critical_count for n in nodes)
        total_warning = sum(n.warning_count for n in nodes)

        p = (
            Point("fleet_summary")
            .tag("space", "bartos-cloud")
            .field("total_nodes", len(nodes))
            .field("reachable_nodes", reachable)
            .field("stale_nodes", stale)
            .field("total_critical_alerts", total_critical)
            .field("total_warning_alerts", total_warning)
            .time(now, "s")
        )

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=[p])
            logger.debug("Wrote fleet summary to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to write fleet summary: {e}")

    def health_check(self) -> bool:
        """Verify InfluxDB connectivity."""
        try:
            health = self.client.health()
            return health.status == "pass"
        except Exception as e:
            logger.error(f"InfluxDB health check failed: {e}")
            return False

    def close(self):
        self.client.close()
