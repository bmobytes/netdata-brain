"""
Netdata Cloud API collector.
Fetches node status and alert counts from Netdata Cloud.
Fetches individual alarm details directly from each node's local agent API.
"""

import logging
import requests
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    id: str
    name: str
    state: str          # reachable | stale | offline
    os: str
    os_name: str
    os_version: str
    cpus: int
    memory: str
    disk_space: str
    virtualization: str
    version: str
    warning_count: int
    critical_count: int
    is_reachable: bool
    node_type: str      # linux | macos | snmp | haos | unknown
    ip: str = ""        # primary IP from hostLabels (_net_default_iface_ip)


@dataclass
class Alert:
    node_id: str
    node_name: str
    name: str
    chart: str
    status: str         # CRITICAL | WARNING | CLEAR
    value: Optional[float]
    value_string: str   # human-readable value (e.g. "100%", "down", "2 disks")
    summary: str        # short description of what triggered
    info: str           # longer description
    last_updated: str


class NetdataCollector:
    BASE_URL = "https://app.netdata.cloud/api/v2"
    AGENT_PORT = 19999
    AGENT_TIMEOUT = 5

    def __init__(self, api_token: str, space_id: str, room_id: str):
        self.api_token = api_token
        self.space_id = space_id
        self.room_id = room_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        })

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = f"{self.BASE_URL}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} fetching {url}: {e.response.text[:200]}")
            raise
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise

    def _classify_node_type(self, node: dict) -> str:
        labels = node.get("hostLabels") or {}
        vnode_type = labels.get("_vnode_type", "")
        os_name = node.get("osName", "").lower()

        if vnode_type == "snmp":
            return "snmp"
        elif "macos" in os_name or node.get("os", "") == "macos":
            return "macos"
        elif "home assistant" in os_name:
            return "haos"
        elif node.get("os", "") == "linux":
            return "linux"
        else:
            return "unknown"

    def fetch_nodes(self) -> list[NodeInfo]:
        """Fetch all nodes from the room."""
        path = f"/spaces/{self.space_id}/rooms/{self.room_id}/nodes"
        try:
            data = self._get(path)
            nodes = []
            for n in data:
                alarm = n.get("alarmCounters", {})
                labels = n.get("hostLabels") or {}
                nodes.append(NodeInfo(
                    id=n.get("id", ""),
                    name=n.get("name", "unknown"),
                    state=n.get("state", "unknown"),
                    os=n.get("os", ""),
                    os_name=n.get("osName", ""),
                    os_version=n.get("osVersion", ""),
                    cpus=n.get("cpus", 0),
                    memory=n.get("memory", "0 B"),
                    disk_space=n.get("diskSpace", "0 B"),
                    virtualization=n.get("virtualization", "unknown"),
                    version=n.get("version", ""),
                    warning_count=alarm.get("warning", 0),
                    critical_count=alarm.get("critical", 0),
                    is_reachable=n.get("state") == "reachable",
                    node_type=self._classify_node_type(n),
                    ip=labels.get("_net_default_iface_ip", ""),
                ))
            logger.info(f"Fetched {len(nodes)} nodes ({sum(1 for n in nodes if n.is_reachable)} reachable)")
            return nodes
        except Exception as e:
            logger.error(f"Failed to fetch nodes: {e}")
            return []

    def fetch_node_alarms(self, node: NodeInfo) -> list[Alert]:
        """
        Fetch active alarms directly from a node's local Netdata agent API.
        The Cloud API only exposes alarm counts — details live on the agent itself.
        Returns empty list if the node has no IP or is unreachable.
        """
        if not node.ip:
            logger.warning(f"No IP for node {node.name}, cannot fetch alarm details")
            return []

        url = f"http://{node.ip}:{self.AGENT_PORT}/api/v1/alarms"
        try:
            resp = requests.get(url, params={"active": True}, timeout=self.AGENT_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            alarms_raw = data.get("alarms", {})

            alerts = []
            for alarm_key, a in alarms_raw.items():
                status = a.get("status", "")
                if status not in ("CRITICAL", "WARNING"):
                    continue
                alerts.append(Alert(
                    node_id=node.id,
                    node_name=node.name,
                    name=a.get("name", alarm_key),
                    chart=a.get("chart", ""),
                    status=status,
                    value=a.get("value"),
                    value_string=a.get("value_string", ""),
                    summary=a.get("summary", ""),
                    info=a.get("info", ""),
                    last_updated=str(a.get("last_status_change", "")),
                ))

            logger.info(f"Fetched {len(alerts)} active alarms from {node.name} ({node.ip})")
            return alerts

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching alarms from {node.name} ({node.ip})")
            return []
        except Exception as e:
            logger.warning(f"Could not fetch alarms from {node.name} ({node.ip}): {e}")
            return []

    def get_space_info(self) -> dict:
        """Get basic space info for startup summary."""
        try:
            spaces = self._get("/spaces")
            for s in spaces:
                if s.get("id") == self.space_id:
                    return s
        except Exception:
            pass
        return {}
