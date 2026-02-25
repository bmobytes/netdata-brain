"""
Discord webhook alerter.
Posts alerts for node state changes and critical/warning transitions.
"""

import logging
import requests
from datetime import datetime, timezone
from collector import NodeInfo, Alert

logger = logging.getLogger(__name__)

# Color codes for Discord embeds
COLOR_CRITICAL = 0xFF0000   # red
COLOR_WARNING  = 0xFFA500   # orange
COLOR_RECOVERY = 0x00C851   # green
COLOR_INFO     = 0x3399FF   # blue
COLOR_OFFLINE  = 0x555555   # grey


class DiscordAlerter:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.enabled = bool(webhook_url)
        if not self.enabled:
            logger.warning("No Discord webhook configured — alerts will log to stdout only")

    def _post(self, payload: dict):
        if not self.enabled:
            logger.info(f"[ALERT] {payload.get('content', payload)}")
            return
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to post Discord alert: {e}")

    def _ts(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def send_startup_summary(self, nodes: list[NodeInfo]):
        reachable = [n for n in nodes if n.is_reachable]
        stale = [n for n in nodes if not n.is_reachable]
        critical_nodes = [n for n in nodes if n.critical_count > 0]
        warning_nodes = [n for n in nodes if n.warning_count > 0]

        fields = [
            {"name": "🟢 Online", "value": str(len(reachable)), "inline": True},
            {"name": "🔴 Offline/Stale", "value": str(len(stale)), "inline": True},
            {"name": "⚠️ With Alerts", "value": str(len(warning_nodes) + len(critical_nodes)), "inline": True},
        ]

        if critical_nodes:
            names = "\n".join(f"• `{n.name}` ({n.critical_count} crit)" for n in critical_nodes[:5])
            fields.append({"name": "🚨 Critical Now", "value": names, "inline": False})

        if stale:
            names = "\n".join(f"• `{n.name}`" for n in stale[:5])
            fields.append({"name": "💤 Stale Nodes", "value": names, "inline": False})

        payload = {
            "username": "netdata-brain",
            "embeds": [{
                "title": "🧠 netdata-brain Online",
                "description": f"Monitoring **{len(nodes)} nodes** in Bartos Cloud",
                "color": COLOR_INFO,
                "fields": fields,
                "footer": {"text": "netdata-brain • startup"},
                "timestamp": self._ts(),
            }]
        }
        self._post(payload)

    def send_node_down(self, node: NodeInfo):
        payload = {
            "username": "netdata-brain",
            "embeds": [{
                "title": f"🔴 Node Offline: {node.name}",
                "description": f"**{node.name}** has gone stale/offline",
                "color": COLOR_OFFLINE,
                "fields": [
                    {"name": "OS", "value": f"{node.os_name} {node.os_version}", "inline": True},
                    {"name": "Type", "value": node.node_type, "inline": True},
                    {"name": "State", "value": node.state, "inline": True},
                ],
                "footer": {"text": "netdata-brain • node down"},
                "timestamp": self._ts(),
            }]
        }
        self._post(payload)

    def send_node_up(self, node: NodeInfo):
        payload = {
            "username": "netdata-brain",
            "embeds": [{
                "title": f"🟢 Node Recovered: {node.name}",
                "description": f"**{node.name}** is back online",
                "color": COLOR_RECOVERY,
                "fields": [
                    {"name": "OS", "value": f"{node.os_name} {node.os_version}", "inline": True},
                    {"name": "Version", "value": node.version, "inline": True},
                ],
                "footer": {"text": "netdata-brain • node recovered"},
                "timestamp": self._ts(),
            }]
        }
        self._post(payload)

    def send_new_critical(self, node: NodeInfo, prev_critical: int):
        new_count = node.critical_count - prev_critical
        payload = {
            "username": "netdata-brain",
            "embeds": [{
                "title": f"🚨 New Critical Alert: {node.name}",
                "description": f"**{node.name}** now has **{node.critical_count}** critical alert(s)",
                "color": COLOR_CRITICAL,
                "fields": [
                    {"name": "New Criticals", "value": str(new_count), "inline": True},
                    {"name": "Total Critical", "value": str(node.critical_count), "inline": True},
                    {"name": "Warnings", "value": str(node.warning_count), "inline": True},
                    {"name": "OS", "value": f"{node.os_name}", "inline": True},
                ],
                "footer": {"text": "netdata-brain • critical alert"},
                "timestamp": self._ts(),
            }]
        }
        self._post(payload)

    def send_critical_cleared(self, node: NodeInfo):
        payload = {
            "username": "netdata-brain",
            "embeds": [{
                "title": f"✅ Critical Cleared: {node.name}",
                "description": f"**{node.name}** critical alerts resolved (warnings: {node.warning_count})",
                "color": COLOR_RECOVERY,
                "footer": {"text": "netdata-brain • alert cleared"},
                "timestamp": self._ts(),
            }]
        }
        self._post(payload)

    def send_daily_digest(self, nodes: list[NodeInfo]):
        reachable = [n for n in nodes if n.is_reachable]
        stale = [n for n in nodes if not n.is_reachable]
        critical = [n for n in nodes if n.critical_count > 0]
        warning = [n for n in nodes if n.warning_count > 0]

        status_emoji = "🟢" if not critical and not stale else ("🔴" if critical else "🟡")

        fields = [
            {"name": "Total Nodes", "value": str(len(nodes)), "inline": True},
            {"name": "🟢 Online", "value": str(len(reachable)), "inline": True},
            {"name": "🔴 Offline", "value": str(len(stale)), "inline": True},
            {"name": "🚨 Critical", "value": str(len(critical)), "inline": True},
            {"name": "⚠️ Warning", "value": str(len(warning)), "inline": True},
        ]

        if critical:
            names = "\n".join(f"• `{n.name}` ({n.critical_count}🚨 {n.warning_count}⚠️)" for n in critical)
            fields.append({"name": "Nodes Needing Attention", "value": names, "inline": False})

        payload = {
            "username": "netdata-brain",
            "embeds": [{
                "title": f"{status_emoji} Daily Fleet Digest — Bartos Cloud",
                "color": COLOR_CRITICAL if critical else (COLOR_WARNING if warning else COLOR_RECOVERY),
                "fields": fields,
                "footer": {"text": "netdata-brain • daily digest"},
                "timestamp": self._ts(),
            }]
        }
        self._post(payload)
