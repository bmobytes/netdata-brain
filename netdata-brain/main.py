"""
netdata-brain — main service loop.

Polls Netdata Cloud API, writes to InfluxDB, and posts Discord alerts
for node state changes and new critical alerts.
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from collector import NetdataCollector, NodeInfo
from writer import InfluxWriter
from alerter import DiscordAlerter

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("netdata-brain")


# ── Config ─────────────────────────────────────────────────────────────────────
def require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        logger.error(f"Required env var {key} is not set")
        sys.exit(1)
    return val


NETDATA_API     = require_env("NETDATA_API")
NETDATA_SPACE   = require_env("NETDATA_SPACE_ID")
NETDATA_ROOM    = require_env("NETDATA_ROOM_ID")
INFLUXDB_URL    = require_env("INFLUXDB_URL")
INFLUXDB_TOKEN  = require_env("INFLUXDB_TOKEN")
INFLUXDB_ORG    = require_env("INFLUXDB_ORG")
INFLUXDB_BUCKET = require_env("INFLUXDB_BUCKET")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "")
POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", "60"))
DAILY_DIGEST_HOUR = int(os.environ.get("DAILY_DIGEST_HOUR", "9"))  # 9am UTC


# ── State tracking ──────────────────────────────────────────────────────────────
class StateTracker:
    """Track previous node states to detect transitions."""

    def __init__(self):
        self.prev_states: dict[str, str] = {}         # node_id → state
        self.prev_critical: dict[str, int] = {}       # node_id → critical_count
        self.last_digest_day: int = -1

    def detect_changes(self, nodes: list[NodeInfo]) -> dict:
        """Returns dict of changes detected this poll cycle."""
        changes = {
            "went_offline": [],
            "came_online": [],
            "new_critical": [],
            "critical_cleared": [],
        }

        for node in nodes:
            prev_state = self.prev_states.get(node.id)
            prev_crit = self.prev_critical.get(node.id, 0)

            # State transitions
            if prev_state is not None:
                if prev_state == "reachable" and not node.is_reachable:
                    changes["went_offline"].append(node)
                elif prev_state != "reachable" and node.is_reachable:
                    changes["came_online"].append(node)

            # Critical alert transitions (only for reachable nodes)
            if node.is_reachable and prev_state is not None:
                if node.critical_count > prev_crit:
                    changes["new_critical"].append((node, prev_crit))
                elif node.critical_count == 0 and prev_crit > 0:
                    changes["critical_cleared"].append(node)

            # Update state
            self.prev_states[node.id] = node.state
            self.prev_critical[node.id] = node.critical_count

        return changes

    def should_send_digest(self) -> bool:
        now = datetime.now(timezone.utc)
        if now.hour == DAILY_DIGEST_HOUR and now.day != self.last_digest_day:
            self.last_digest_day = now.day
            return True
        return False


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  netdata-brain starting up")
    logger.info(f"  Poll interval: {POLL_INTERVAL}s")
    logger.info(f"  InfluxDB: {INFLUXDB_URL}")
    logger.info(f"  Discord alerts: {'enabled' if DISCORD_WEBHOOK else 'disabled (no webhook)'}")
    logger.info("=" * 60)

    # Initialize components
    collector = NetdataCollector(NETDATA_API, NETDATA_SPACE, NETDATA_ROOM)
    alerter = DiscordAlerter(DISCORD_WEBHOOK)
    tracker = StateTracker()

    # Wait for InfluxDB to be ready (it can take a moment on first start)
    writer = None
    for attempt in range(10):
        try:
            writer = InfluxWriter(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET)
            if writer.health_check():
                logger.info("InfluxDB connection verified")
                break
        except Exception as e:
            logger.warning(f"InfluxDB not ready (attempt {attempt + 1}/10): {e}")
        time.sleep(6)
    else:
        logger.error("Could not connect to InfluxDB after 10 attempts — metrics will be skipped")

    # Graceful shutdown
    running = True
    def shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received")
        running = False
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # First poll — startup summary
    logger.info("Running initial poll...")
    nodes = collector.fetch_nodes()
    if nodes:
        # Log fleet summary
        reachable = sum(1 for n in nodes if n.is_reachable)
        critical_nodes = [n for n in nodes if n.critical_count > 0]
        logger.info(f"Fleet: {len(nodes)} nodes | {reachable} reachable | "
                    f"{len(critical_nodes)} with critical alerts")
        for n in critical_nodes:
            logger.warning(f"  CRITICAL: {n.name} ({n.critical_count} crit, {n.warning_count} warn)")

        # Initialize state tracker without triggering alerts
        for node in nodes:
            tracker.prev_states[node.id] = node.state
            tracker.prev_critical[node.id] = node.critical_count

        # Write initial data
        if writer:
            writer.write_nodes(nodes)
            writer.write_fleet_summary(nodes)

        # Send startup summary to Discord
        alerter.send_startup_summary(nodes)
    else:
        logger.warning("No nodes returned on initial poll")

    # Main loop
    while running:
        time.sleep(POLL_INTERVAL)
        if not running:
            break

        try:
            nodes = collector.fetch_nodes()
            if not nodes:
                logger.warning("Empty node list returned — skipping this cycle")
                continue

            # Detect state changes and alert
            changes = tracker.detect_changes(nodes)

            for node in changes["went_offline"]:
                logger.warning(f"NODE DOWN: {node.name}")
                alerter.send_node_down(node)

            for node in changes["came_online"]:
                logger.info(f"NODE UP: {node.name}")
                alerter.send_node_up(node)

            for node, prev_crit in changes["new_critical"]:
                logger.warning(f"NEW CRITICAL: {node.name} ({node.critical_count} alerts)")
                alerter.send_new_critical(node, prev_crit)

            for node in changes["critical_cleared"]:
                logger.info(f"CRITICAL CLEARED: {node.name}")
                alerter.send_critical_cleared(node)

            # Write metrics
            if writer:
                writer.write_nodes(nodes)
                writer.write_fleet_summary(nodes)

            # Daily digest
            if tracker.should_send_digest():
                logger.info("Sending daily digest")
                alerter.send_daily_digest(nodes)

            reachable = sum(1 for n in nodes if n.is_reachable)
            total_crit = sum(n.critical_count for n in nodes)
            logger.info(f"Poll complete: {reachable}/{len(nodes)} reachable, {total_crit} critical alerts")

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error in poll cycle: {e}", exc_info=True)

    logger.info("netdata-brain shutting down")
    if writer:
        writer.close()


if __name__ == "__main__":
    main()
