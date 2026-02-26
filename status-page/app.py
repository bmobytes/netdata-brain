"""
netdata-brain status page.
Serves a live fleet overview dashboard.
"""

import csv
import io
import os
import time
import logging
import requests
from flask import Flask, render_template_string
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("status-page")

app = Flask(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
NETDATA_API   = os.environ.get("NETDATA_API", "")
SPACE_ID      = os.environ.get("NETDATA_SPACE_ID", "")
ROOM_ID       = os.environ.get("NETDATA_ROOM_ID", "")
CACHE_SECONDS = int(os.environ.get("CACHE_SECONDS", "30"))
API_BASE      = "https://app.netdata.cloud/api/v2"

INFLUX_URL    = os.environ.get("INFLUX_URL", "")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN", "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG", "bartos")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "netdata")

# ── Simple in-memory caches ────────────────────────────────────────────────────
_cache        = {"nodes": [], "fetched_at": 0, "error": None}
_alarm_cache  = {"data": {}, "fetched_at": 0}   # {node_name: [{name, severity, silenced, summary}]}
_unifi_cache  = {"data": [], "fetched_at": 0}   # [{mac, name, model, device_type, state, uptime, clients}]


# ── Netdata Cloud: node list ───────────────────────────────────────────────────
def fetch_nodes():
    now = time.time()
    if now - _cache["fetched_at"] < CACHE_SECONDS and _cache["nodes"]:
        return _cache["nodes"], None

    try:
        resp = requests.get(
            f"{API_BASE}/spaces/{SPACE_ID}/rooms/{ROOM_ID}/nodes",
            headers={"Authorization": f"Bearer {NETDATA_API}"},
            timeout=10,
        )
        resp.raise_for_status()
        nodes = resp.json()
        _cache["nodes"] = nodes
        _cache["fetched_at"] = now
        _cache["error"] = None
        return nodes, None
    except Exception as e:
        logger.error(f"Failed to fetch nodes: {e}")
        _cache["error"] = str(e)
        return _cache["nodes"], str(e)


# ── InfluxDB helpers ───────────────────────────────────────────────────────────
def query_influx(flux: str) -> list[dict]:
    """
    Run a Flux query against InfluxDB v2. Returns list of row dicts.
    Handles the annotated CSV format (multiple tables, #comment rows).
    """
    if not INFLUX_URL or not INFLUX_TOKEN:
        return []
    try:
        resp = requests.post(
            f"{INFLUX_URL}/api/v2/query",
            params={"org": INFLUX_ORG},
            headers={
                "Authorization": f"Token {INFLUX_TOKEN}",
                "Content-Type": "application/vnd.flux",
                "Accept": "application/csv",
            },
            data=flux,
            timeout=10,
        )
        resp.raise_for_status()

        # InfluxDB annotated CSV: lines starting with # are annotations.
        # Each table block starts with annotation rows, then a header row, then data rows.
        # Tables separated by blank lines. We parse each block separately.
        results = []
        current_header = None
        for line in resp.text.splitlines():
            if line.startswith("#"):
                continue
            if not line.strip():
                current_header = None  # blank line = end of table block
                continue
            row_vals = next(csv.reader([line]))
            if current_header is None:
                current_header = row_vals
                continue
            if len(row_vals) == len(current_header):
                results.append(dict(zip(current_header, row_vals)))
        return results
    except Exception as e:
        logger.error(f"InfluxDB query failed: {e}")
        return []


def fetch_alarms() -> dict:
    """
    Query InfluxDB for recent active alarms per node.
    Returns: {node_name: [{name, severity, silenced, summary}]}
    """
    now = time.time()
    if now - _alarm_cache["fetched_at"] < CACHE_SECONDS:
        return _alarm_cache["data"]

    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "node_alarms")
  |> group(columns: ["node_name", "alarm_name", "_field"])
  |> last()
"""
    rows = query_influx(flux)
    raw = {}  # {(node_name, alarm_name): {field: value}}
    for row in rows:
        node  = row.get("node_name",  "").strip()
        alarm = row.get("alarm_name", "").strip()
        field = row.get("_field",     "").strip()
        value = row.get("_value",     "").strip()
        if node and alarm and field:
            key = (node, alarm)
            raw.setdefault(key, {
                "name": alarm, "severity": "1", "silenced": "0", "summary": ""
            })
            raw[key][field] = value

    result = {}
    for (node, alarm), data in raw.items():
        result.setdefault(node, []).append(data)

    # Sort each node's alarms: critical first, then warning, then silenced
    for alarms in result.values():
        alarms.sort(key=lambda a: (int(a.get("silenced", "0")), -int(a.get("severity", "1"))))

    _alarm_cache["data"] = result
    _alarm_cache["fetched_at"] = now
    return result


def fetch_unifi_devices() -> list:
    """
    Query InfluxDB for latest UniFi device status.
    Returns: [{mac, name, model, device_type, state, uptime, clients}]
    """
    now = time.time()
    if now - _unifi_cache["fetched_at"] < CACHE_SECONDS:
        return _unifi_cache["data"]

    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -3m)
  |> filter(fn: (r) => r._measurement == "unifi_device")
  |> group(columns: ["mac", "name", "model", "device_type", "_field"])
  |> last()
"""
    rows = query_influx(flux)
    devices = {}  # {mac: {...}}
    for row in rows:
        mac   = row.get("mac",         "").strip()
        field = row.get("_field",      "").strip()
        value = row.get("_value",      "").strip()
        if mac and field:
            devices.setdefault(mac, {
                "mac":         mac,
                "name":        row.get("name",        mac),
                "model":       row.get("model",       ""),
                "device_type": row.get("device_type", ""),
                "state":       "0",
                "uptime":      "0",
                "clients":     "0",
            })
            devices[mac][field] = value
            # Tags are also repeated per row — keep them fresh
            for tag in ("name", "model", "device_type"):
                if row.get(tag):
                    devices[mac][tag] = row[tag]

    # Sort: by state desc (online first), then by name
    result = sorted(
        devices.values(),
        key=lambda d: (-int(d.get("state", 0)), d.get("name", "").lower())
    )
    _unifi_cache["data"] = result
    _unifi_cache["fetched_at"] = now
    return result


# ── Classification helpers ─────────────────────────────────────────────────────
def classify_node(node):
    alarm = node.get("alarmCounters", {})
    crit  = alarm.get("critical", 0)
    warn  = alarm.get("warning",  0)
    state = node.get("state", "unknown")

    if state != "reachable":
        return "offline"
    elif crit > 0:
        return "critical"
    elif warn > 0:
        return "warning"
    else:
        return "ok"


def node_type_icon(node):
    labels  = node.get("hostLabels") or {}
    vtype   = labels.get("_vnode_type", "")
    os_val  = node.get("os", "")
    os_name = node.get("osName", "").lower()

    if vtype == "snmp":
        return "🔌"
    elif "macos" in os_name or os_val == "macos":
        return "🍎"
    elif "home assistant" in os_name:
        return "🏠"
    elif os_val == "linux":
        return "🐧"
    return "❓"


def unifi_type_icon(dev):
    dtype = (dev.get("device_type") or "").lower()
    model = (dev.get("model")       or "").lower()
    if "ugw" in dtype or "ucg" in model or "udm" in model or "udma" in model:
        return "🛡️"
    elif "usw" in dtype:
        return "🔀"
    elif "uap" in dtype:
        return "📡"
    return "🌐"


def humanize_uptime(seconds_str) -> str:
    try:
        secs = int(float(seconds_str))
    except (ValueError, TypeError):
        return "?"
    d, r = divmod(secs, 86400)
    h, r = divmod(r, 3600)
    m, _ = divmod(r, 60)
    if d > 0:
        return f"{d}d {h}h"
    elif h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


# ── HTML template ──────────────────────────────────────────────────────────────
HTML_TEMPLATE = """
{% macro render_node(node) %}
{% set status = node._status %}
{% set alarm  = node.alarmCounters or {} %}
{% set alarms = node._alarms %}
<div class="node {{ status }}">
  <div class="dot {{ status }}"></div>
  <div class="node-info">
    <div class="node-name">{{ node_type_icon(node) }} {{ node.name }}</div>
    <div class="node-meta">{{ node.osName or node.os }} &nbsp;•&nbsp; {{ node.cpus }}c / {{ node.memory }}</div>
    <div class="badges">
      <span class="badge type">{{ node._type }}</span>
      {% if alarms %}
        {% for a in alarms %}
          {% set sev = a.severity | int %}
          {% set acked = a.silenced | int %}
          {% if acked %}
            <span class="badge ack" title="{{ a.summary }}">✓ {{ a.summary[:40] }}</span>
          {% elif sev == 2 %}
            <span class="badge crit" title="{{ a.summary }}">🔴 {{ a.summary[:40] }}</span>
          {% else %}
            <span class="badge warn" title="{{ a.summary }}">⚠️ {{ a.summary[:40] }}</span>
          {% endif %}
        {% endfor %}
      {% else %}
        {# Fallback: count-based from Netdata Cloud (no detail data yet) #}
        {% if alarm.critical > 0 %}<span class="badge crit">{{ alarm.critical }} critical</span>{% endif %}
        {% if alarm.warning  > 0 %}<span class="badge warn">{{ alarm.warning }} warning</span>{% endif %}
      {% endif %}
    </div>
  </div>
</div>
{% endmacro %}

{% macro render_unifi(dev) %}
{% set up = dev.state == '1' %}
<div class="node {{ 'ok' if up else 'offline' }}">
  <div class="dot {{ 'ok' if up else 'offline' }}"></div>
  <div class="node-info">
    <div class="node-name">{{ unifi_type_icon(dev) }} {{ dev.name }}</div>
    <div class="node-meta">{{ dev.model }} &nbsp;•&nbsp; {{ dev.device_type | upper }}</div>
    <div class="badges">
      <span class="badge type">UniFi</span>
      <span class="badge type">{{ dev.clients }} clients</span>
      {% if dev.uptime != '0' %}<span class="badge type">↑ {{ humanize_uptime(dev.uptime) }}</span>{% endif %}
    </div>
  </div>
</div>
{% endmacro %}

<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="{{ refresh }}">
<title>Bartos Cloud — Fleet Status</title>
<style>
  :root {
    --bg:      #0d1117;
    --surface: #161b22;
    --border:  #30363d;
    --text:    #e6edf3;
    --muted:   #8b949e;
    --green:   #3fb950;
    --yellow:  #d29922;
    --red:     #f85149;
    --grey:    #484f58;
    --blue:    #388bfd;
    --purple:  #8957e5;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 24px; }
  h1 { font-size: 1.4rem; font-weight: 600; margin-bottom: 4px; }
  .subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 24px; }
  .summary { display: flex; gap: 12px; margin-bottom: 28px; flex-wrap: wrap; }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px 20px; min-width: 110px; text-align: center; }
  .stat .num { font-size: 2rem; font-weight: 700; line-height: 1; }
  .stat .label { color: var(--muted); font-size: 0.75rem; margin-top: 4px; }
  .stat.ok   .num { color: var(--green); }
  .stat.warn .num { color: var(--yellow); }
  .stat.crit .num { color: var(--red); }
  .stat.off  .num { color: var(--grey); }
  .section-title { font-size: 0.8rem; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: var(--muted); margin: 20px 0 10px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 10px; margin-bottom: 24px; }
  .node { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; display: flex; align-items: flex-start; gap: 12px; }
  .node.critical { border-color: var(--red); }
  .node.warning  { border-color: var(--yellow); }
  .node.offline  { opacity: 0.55; }
  .dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; margin-top: 4px; }
  .dot.ok       { background: var(--green); }
  .dot.warning  { background: var(--yellow); }
  .dot.critical { background: var(--red); animation: pulse 2s ease-in-out infinite; }
  .dot.offline  { background: var(--grey); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  .node-info { flex: 1; min-width: 0; }
  .node-name { font-size: 0.9rem; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .node-meta { font-size: 0.75rem; color: var(--muted); margin-top: 2px; }
  .badges { display: flex; gap: 4px; margin-top: 6px; flex-wrap: wrap; }
  .badge { font-size: 0.7rem; padding: 2px 7px; border-radius: 4px; font-weight: 500; max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: default; }
  .badge.crit { background: rgba(248,81,73,0.15);  color: var(--red);    border: 1px solid rgba(248,81,73,0.3); }
  .badge.warn { background: rgba(210,153,34,0.15); color: var(--yellow); border: 1px solid rgba(210,153,34,0.3); }
  .badge.ack  { background: rgba(72,79,88,0.3);    color: var(--muted);  border: 1px solid rgba(72,79,88,0.5);  font-style: italic; }
  .badge.type { background: rgba(56,139,253,0.1);  color: var(--blue);   border: 1px solid rgba(56,139,253,0.2); }
  .error-banner { background: rgba(248,81,73,0.1); border: 1px solid rgba(248,81,73,0.3); border-radius: 8px; padding: 10px 16px; margin-bottom: 20px; color: var(--red); font-size: 0.85rem; }
  .divider { border: none; border-top: 1px solid var(--border); margin: 28px 0 8px; }
  .footer { color: var(--muted); font-size: 0.75rem; margin-top: 32px; }
</style>
</head>
<body>
<h1>🧠 Bartos Cloud — Fleet Status</h1>
<p class="subtitle">Auto-refreshes every {{ refresh }}s &nbsp;•&nbsp; Last updated: {{ updated }}</p>

{% if error %}
<div class="error-banner">⚠️ API error: {{ error }} — showing cached data</div>
{% endif %}

<div class="summary">
  <div class="stat ok">
    <div class="num">{{ counts.ok }}</div>
    <div class="label">Online</div>
  </div>
  <div class="stat warn">
    <div class="num">{{ counts.warning }}</div>
    <div class="label">Warning</div>
  </div>
  <div class="stat crit">
    <div class="num">{{ counts.critical }}</div>
    <div class="label">Critical</div>
  </div>
  <div class="stat off">
    <div class="num">{{ counts.offline }}</div>
    <div class="label">Offline</div>
  </div>
</div>

{% if groups.critical %}
<div class="section-title">🚨 Critical ({{ groups.critical|length }})</div>
<div class="grid">{% for node in groups.critical %}{{ render_node(node) }}{% endfor %}</div>
{% endif %}

{% if groups.warning %}
<div class="section-title">⚠️ Warning ({{ groups.warning|length }})</div>
<div class="grid">{% for node in groups.warning %}{{ render_node(node) }}{% endfor %}</div>
{% endif %}

{% if groups.offline %}
<div class="section-title">💤 Offline / Stale ({{ groups.offline|length }})</div>
<div class="grid">{% for node in groups.offline %}{{ render_node(node) }}{% endfor %}</div>
{% endif %}

{% if groups.ok %}
<div class="section-title">✅ Online ({{ groups.ok|length }})</div>
<div class="grid">{% for node in groups.ok %}{{ render_node(node) }}{% endfor %}</div>
{% endif %}

{% if unifi_devices %}
<hr class="divider">
<div class="section-title">🌐 Network Infrastructure ({{ unifi_devices|length }} devices)</div>
<div class="grid">{% for dev in unifi_devices %}{{ render_unifi(dev) }}{% endfor %}</div>
{% endif %}

<p class="footer">netdata-brain status page &nbsp;•&nbsp; {{ total }} nodes &nbsp;•&nbsp; {{ unifi_devices|length }} network devices</p>
</body>
</html>
"""


# ── Route ──────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    nodes, error = fetch_nodes()
    alarms        = fetch_alarms()        # {node_name: [alarm_dicts]}
    unifi_devices = fetch_unifi_devices()  # [device_dicts]

    # Classify each node + attach alarm detail list
    for node in nodes:
        node["_status"] = classify_node(node)
        node["_type"]   = (node.get("hostLabels") or {}).get("_vnode_type") or node.get("os", "unknown")
        node["_alarms"] = alarms.get(node.get("name", ""), [])

    groups = {"critical": [], "warning": [], "offline": [], "ok": []}
    counts = {"critical": 0,  "warning": 0,  "offline": 0,  "ok": 0}

    for node in nodes:
        s = node["_status"]
        groups[s].append(node)
        counts[s] += 1

    for g in groups.values():
        g.sort(key=lambda n: n.get("name", ""))

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return render_template_string(
        HTML_TEMPLATE,
        groups=groups,
        counts=counts,
        total=len(nodes),
        error=error,
        refresh=CACHE_SECONDS,
        updated=updated,
        unifi_devices=unifi_devices,
        node_type_icon=node_type_icon,
        unifi_type_icon=unifi_type_icon,
        humanize_uptime=humanize_uptime,
    )


@app.route("/health")
def health():
    return {
        "status": "ok",
        "nodes_cached": len(_cache["nodes"]),
        "influx_configured": bool(INFLUX_URL and INFLUX_TOKEN),
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
