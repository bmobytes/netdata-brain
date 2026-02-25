"""
netdata-brain status page.
Serves a live fleet overview dashboard.
"""

import os
import time
import logging
import requests
from flask import Flask, render_template_string
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("status-page")

app = Flask(__name__)

# Config
NETDATA_API    = os.environ.get("NETDATA_API", "")
SPACE_ID       = os.environ.get("NETDATA_SPACE_ID", "")
ROOM_ID        = os.environ.get("NETDATA_ROOM_ID", "")
CACHE_SECONDS  = int(os.environ.get("CACHE_SECONDS", "30"))
API_BASE       = "https://app.netdata.cloud/api/v2"

# Simple in-memory cache
_cache = {"nodes": [], "fetched_at": 0, "error": None}


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
        return _cache["nodes"], str(e)  # return stale data + error


def classify_node(node):
    alarm = node.get("alarmCounters", {})
    crit = alarm.get("critical", 0)
    warn = alarm.get("warning", 0)
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
    labels = node.get("hostLabels") or {}
    vtype = labels.get("_vnode_type", "")
    os_val = node.get("os", "")
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


HTML_TEMPLATE = """
{% macro render_node(node) %}
{% set status = node._status %}
{% set alarm = node.alarmCounters or {} %}
<div class="node {{ status }}">
  <div class="dot {{ status }}"></div>
  <div class="node-info">
    <div class="node-name">{{ node_type_icon(node) }} {{ node.name }}</div>
    <div class="node-meta">{{ node.osName or node.os }} &nbsp;•&nbsp; {{ node.cpus }}c / {{ node.memory }}</div>
    <div class="badges">
      {% if alarm.critical > 0 %}<span class="badge crit">{{ alarm.critical }} critical</span>{% endif %}
      {% if alarm.warning > 0 %}<span class="badge warn">{{ alarm.warning }} warning</span>{% endif %}
      <span class="badge type">{{ node._type }}</span>
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
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --green: #3fb950;
    --yellow: #d29922;
    --red: #f85149;
    --grey: #484f58;
    --blue: #388bfd;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 24px; }
  h1 { font-size: 1.4rem; font-weight: 600; margin-bottom: 4px; }
  .subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 24px; }
  .summary { display: flex; gap: 12px; margin-bottom: 28px; flex-wrap: wrap; }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px 20px; min-width: 110px; text-align: center; }
  .stat .num { font-size: 2rem; font-weight: 700; line-height: 1; }
  .stat .label { color: var(--muted); font-size: 0.75rem; margin-top: 4px; }
  .stat.ok .num { color: var(--green); }
  .stat.warn .num { color: var(--yellow); }
  .stat.crit .num { color: var(--red); }
  .stat.off .num { color: var(--grey); }
  .section-title { font-size: 0.8rem; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: var(--muted); margin: 20px 0 10px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; margin-bottom: 24px; }
  .node { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; display: flex; align-items: center; gap: 12px; position: relative; }
  .node.critical { border-color: var(--red); }
  .node.warning  { border-color: var(--yellow); }
  .node.offline  { opacity: 0.55; }
  .dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .dot.ok       { background: var(--green); }
  .dot.warning  { background: var(--yellow); }
  .dot.critical { background: var(--red); }
  .dot.offline  { background: var(--grey); }
  .node-info { flex: 1; min-width: 0; }
  .node-name { font-size: 0.9rem; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .node-meta { font-size: 0.75rem; color: var(--muted); margin-top: 2px; }
  .badges { display: flex; gap: 4px; margin-top: 4px; flex-wrap: wrap; }
  .badge { font-size: 0.7rem; padding: 1px 6px; border-radius: 4px; font-weight: 500; }
  .badge.crit { background: rgba(248,81,73,0.15); color: var(--red); border: 1px solid rgba(248,81,73,0.3); }
  .badge.warn { background: rgba(210,153,34,0.15); color: var(--yellow); border: 1px solid rgba(210,153,34,0.3); }
  .badge.type { background: rgba(56,139,253,0.1); color: var(--blue); border: 1px solid rgba(56,139,253,0.2); }
  .error-banner { background: rgba(248,81,73,0.1); border: 1px solid rgba(248,81,73,0.3); border-radius: 8px; padding: 10px 16px; margin-bottom: 20px; color: var(--red); font-size: 0.85rem; }
  .footer { color: var(--muted); font-size: 0.75rem; margin-top: 32px; }
  .stale-note { font-size: 0.7rem; color: var(--grey); margin-top: 2px; }
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
<div class="grid">
{% for node in groups.critical %}
  {{ render_node(node) }}
{% endfor %}
</div>
{% endif %}

{% if groups.warning %}
<div class="section-title">⚠️ Warning ({{ groups.warning|length }})</div>
<div class="grid">
{% for node in groups.warning %}
  {{ render_node(node) }}
{% endfor %}
</div>
{% endif %}

{% if groups.offline %}
<div class="section-title">💤 Offline / Stale ({{ groups.offline|length }})</div>
<div class="grid">
{% for node in groups.offline %}
  {{ render_node(node) }}
{% endfor %}
</div>
{% endif %}

{% if groups.ok %}
<div class="section-title">✅ Online ({{ groups.ok|length }})</div>
<div class="grid">
{% for node in groups.ok %}
  {{ render_node(node) }}
{% endfor %}
</div>
{% endif %}

<p class="footer">netdata-brain status page &nbsp;•&nbsp; {{ total }} nodes total</p>
</body>
</html>
"""


@app.route("/")
def index():
    nodes, error = fetch_nodes()

    # Classify each node
    for node in nodes:
        node["_status"] = classify_node(node)
        node["_type"] = (node.get("hostLabels") or {}).get("_vnode_type") or node.get("os", "unknown")

    groups = {"critical": [], "warning": [], "offline": [], "ok": []}
    counts = {"critical": 0, "warning": 0, "offline": 0, "ok": 0}

    for node in nodes:
        s = node["_status"]
        groups[s].append(node)
        counts[s] += 1

    # Sort each group by name
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
        node_type_icon=node_type_icon,
    )


@app.route("/health")
def health():
    return {"status": "ok", "nodes_cached": len(_cache["nodes"])}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
