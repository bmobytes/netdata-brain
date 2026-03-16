#!/usr/bin/env python3
"""
TV Dashboard Builder — Bartos Homelab
Builds and pushes 3 TV-optimized 1080p Grafana dashboards + playlist.

Dashboards:
  1. Home Status    — safety, temps, HVAC, power, weather
  2. Homelab Status — fleet health, TrueNAS, UPS, alarms, UniFi
  3. Media & Network— Plex streams, library, top clients, bandwidth

Usage:
  python3 build_tv_dashboards.py [--dry-run]
"""

import json
import sys
import urllib.request
import urllib.error

GRAFANA_URL = "http://192.168.148.185:3001"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "bmografana2026"
DS_UID = "cfecbpgdhs5j4b"  # InfluxDB datasource UID (all buckets share one datasource)

DS = {"type": "influxdb", "uid": DS_UID}

# ──────────────────────────────────────────────
# Panel builder helpers
# ──────────────────────────────────────────────

def pos(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}

def target(query, ref="A"):
    return {"datasource": DS, "query": query.strip(), "refId": ref}

def thresholds(*steps):
    """steps: (value_or_None, color) pairs"""
    return {
        "mode": "absolute",
        "steps": [{"value": v, "color": c} for v, c in steps]
    }

def stat_panel(title, gpos, query, unit="none", decimals=1, thresh=None,
               color_mode="background", mappings=None, no_value=None,
               text_size=None, graph_mode="none"):
    p = {
        "type": "stat",
        "title": title,
        "gridPos": gpos,
        "options": {
            "reduceOptions": {"calcs": ["last"], "fields": "", "values": False},
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": color_mode,
            "graphMode": graph_mode,
            "justifyMode": "center",
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "color": {"mode": "thresholds"},
                "thresholds": thresh or thresholds((None, "green")),
                "mappings": mappings or [],
                **({"noValue": no_value} if no_value is not None else {}),
            },
            "overrides": [],
        },
        "targets": [target(query)],
    }
    if text_size:
        p["options"]["text"] = {"titleSize": text_size, "valueSize": text_size + 8}
    return p

def gauge_panel(title, gpos, query, unit="percent", min_val=0, max_val=100,
                thresh=None, decimals=0):
    return {
        "type": "gauge",
        "title": title,
        "gridPos": gpos,
        "options": {
            "reduceOptions": {"calcs": ["last"], "fields": "", "values": False},
            "orientation": "auto",
            "showThresholdLabels": False,
            "showThresholdMarkers": True,
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "min": min_val,
                "max": max_val,
                "color": {"mode": "thresholds"},
                "thresholds": thresh or thresholds((None, "green"), (75, "yellow"), (90, "red")),
                "mappings": [],
            },
            "overrides": [],
        },
        "targets": [target(query)],
    }

def bar_gauge_panel(title, gpos, query, unit="none", orient="horizontal",
                    thresh=None, decimals=1, display_mode="gradient",
                    value_mode="color"):
    return {
        "type": "bargauge",
        "title": title,
        "gridPos": gpos,
        "options": {
            "reduceOptions": {"calcs": ["last"], "fields": "", "values": False},
            "orientation": orient,
            "displayMode": display_mode,
            "valueMode": value_mode,
            "showUnfilled": True,
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "color": {"mode": "thresholds"},
                "thresholds": thresh or thresholds((None, "blue"), (50, "green"), (80, "yellow"), (95, "red")),
                "mappings": [],
            },
            "overrides": [],
        },
        "targets": [target(query)],
    }

def timeseries_panel(title, gpos, queries, unit="none", thresh=None,
                     fill_opacity=10, line_width=2, legend=True):
    tgts = []
    for i, (q, lbl) in enumerate(queries if isinstance(queries[0], tuple) else [(q, None) for q in queries]):
        t = {"datasource": DS, "query": q.strip(), "refId": chr(65 + i)}
        if lbl:
            t["legendFormat"] = lbl
        tgts.append(t)
    return {
        "type": "timeseries",
        "title": title,
        "gridPos": gpos,
        "options": {
            "legend": {"displayMode": "list" if legend else "hidden", "placement": "bottom"},
            "tooltip": {"mode": "multi"},
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "palette-classic"},
                "thresholds": thresh or thresholds((None, "green")),
                "custom": {
                    "fillOpacity": fill_opacity,
                    "lineWidth": line_width,
                    "drawStyle": "line",
                    "spanNulls": False,
                },
            },
            "overrides": [],
        },
        "targets": tgts,
    }

def table_panel(title, gpos, query, col_overrides=None):
    return {
        "type": "table",
        "title": title,
        "gridPos": gpos,
        "options": {
            "footer": {"show": False, "reducer": ["sum"]},
            "showHeader": True,
            "sortBy": [],
            "cellHeight": "sm",
        },
        "fieldConfig": {
            "defaults": {"color": {"mode": "thresholds"}, "mappings": []},
            "overrides": col_overrides or [],
        },
        "targets": [target(query)],
    }

def row_panel(title, y):
    return {
        "type": "row",
        "title": title,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "collapsed": False,
        "panels": [],
    }

def text_panel(title, content, gpos, mode="markdown"):
    return {
        "type": "text",
        "title": title,
        "gridPos": gpos,
        "options": {"mode": mode, "content": content},
    }

BINARY_ALERT_MAPPINGS = [
    {"type": "value", "options": {"0": {"text": "✅ Clear", "color": "green", "index": 0},
                                   "1": {"text": "🚨 ALERT", "color": "red", "index": 1}}},
]
DOOR_MAPPINGS = [
    {"type": "value", "options": {"0": {"text": "🟢 Closed", "color": "green", "index": 0},
                                   "1": {"text": "🟡 Open", "color": "yellow", "index": 1}}},
]
HVAC_HEAT_MAPPINGS = [
    {"type": "value", "options": {"0": {"text": "⬜ Idle", "color": "gray", "index": 0},
                                   "1": {"text": "🔥 Heating", "color": "orange", "index": 1}}},
]
ONLINE_MAPPINGS = [
    {"type": "value", "options": {"0": {"text": "🔴 Offline", "color": "red", "index": 0},
                                   "1": {"text": "🟢 Online", "color": "green", "index": 1}}},
]

def ha_last(bucket_field, entity_id, start="-5m"):
    """Return Flux query for last HA value."""
    return f"""
from(bucket: "bartos-homeassistant")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "{bucket_field}" and r._field == "value" and r.entity_id == "{entity_id}")
  |> last()
"""

def ha_binary_last(entity_id, start="-5m"):
    return f"""
from(bucket: "bartos-homeassistant")
  |> range(start: {start})
  |> filter(fn: (r) => r.domain == "binary_sensor" and r._field == "value" and r.entity_id == "{entity_id}")
  |> last()
"""

def dashboard_base(uid, title, refresh="30s", time_from="now-1h", tags=None):
    return {
        "uid": uid,
        "title": title,
        "tags": tags or ["tv", "homelab"],
        "timezone": "browser",
        "refresh": refresh,
        "time": {"from": time_from, "to": "now"},
        "timepicker": {},
        "schemaVersion": 38,
        "version": 1,
        "panels": [],
        "templating": {"list": []},
        "annotations": {"list": []},
        "links": [],
        "style": "dark",
        "graphTooltip": 1,
    }


# ══════════════════════════════════════════════════════════════════
# DASHBOARD 1: HOME STATUS
# ══════════════════════════════════════════════════════════════════

def build_home_status():
    d = dashboard_base("tv-home-status", "🏠 Home Status", tags=["tv", "home"])
    panels = []

    # ── Row 1: Safety & Security (h=4, y=0)
    panels.append(row_panel("🚨 Safety & Security", 0))
    safety_y = 1
    panels += [
        stat_panel("Smoke", pos(0, safety_y, 4, 4),
            ha_binary_last("zcombo_g_smoke_co_alarm_smoke_detected"),
            mappings=BINARY_ALERT_MAPPINGS,
            thresh=thresholds((None, "green"), (1, "red"))),
        stat_panel("CO Alarm", pos(4, safety_y, 4, 4),
            ha_binary_last("zcombo_g_smoke_co_alarm_carbon_monoxide_detected"),
            mappings=BINARY_ALERT_MAPPINGS,
            thresh=thresholds((None, "green"), (1, "red"))),
        stat_panel("Water Leak (Server)", pos(8, safety_y, 4, 4),
            ha_binary_last("server_room_leak_detector_water_leak"),
            mappings=BINARY_ALERT_MAPPINGS,
            thresh=thresholds((None, "green"), (1, "red"))),
        stat_panel("Front Door", pos(12, safety_y, 4, 4),
            ha_binary_last("front_door_contact_sensor_contact"),
            mappings=DOOR_MAPPINGS,
            thresh=thresholds((None, "green"), (1, "yellow"))),
        stat_panel("Back Door", pos(16, safety_y, 4, 4),
            ha_binary_last("back_door_contact_sensor_contact"),
            mappings=DOOR_MAPPINGS,
            thresh=thresholds((None, "green"), (1, "yellow"))),
        stat_panel("Heating", pos(20, safety_y, 4, 4),
            ha_binary_last("smart_thermostat_heating_active"),
            mappings=HVAC_HEAT_MAPPINGS,
            thresh=thresholds((None, "gray"), (1, "orange"))),
    ]

    # ── Row 2: Room Temperatures (h=6, y=5)
    panels.append(row_panel("🌡 Temperatures", 5))
    temp_y = 6
    temp_thresh = thresholds((None, "blue"), (65, "green"), (78, "yellow"), (85, "red"))
    room_temps = [
        ("Outdoor", "lansing_st_weather_temp"),
        ("Living Room", "living_room_temperature"),
        ("Bedroom", "bedroom_temperature"),
        ("Office", "office_temperature"),
        ("Basement", "basement_temperature"),
        ("Kitchen", "kitchen_zigbee_temperature_temperature"),
    ]
    for i, (lbl, eid) in enumerate(room_temps):
        panels.append(stat_panel(lbl, pos(i * 4, temp_y, 4, 6),
            ha_last("°F", eid), unit="fahrenheit", decimals=1,
            thresh=temp_thresh, color_mode="background"))

    # ── Row 3: HVAC & Humidity (h=4, y=12)
    panels.append(row_panel("❄️ HVAC & Humidity", 12))
    hvac_y = 13
    panels += [
        stat_panel("Thermostat Set", pos(0, hvac_y, 4, 4),
            ha_last("°F", "smart_thermostat_effective_temperature_fahrenheit"),
            unit="fahrenheit", decimals=0,
            thresh=thresholds((None, "blue"), (68, "green"), (74, "yellow"))),
        stat_panel("HVAC Mode", pos(4, hvac_y, 4, 4),
            f"""
from(bucket: "bartos-homeassistant")
  |> range(start: -5m)
  |> filter(fn: (r) => r.domain == "climate" and r._field == "hvac_action_str" and r.entity_id == "smart_thermostat")
  |> last()
""",
            unit="string", decimals=0,
            thresh=thresholds((None, "blue"))),
        stat_panel("House Humidity", pos(8, hvac_y, 4, 4),
            ha_last("%", "average_house_humidity"),
            unit="percent", decimals=0,
            thresh=thresholds((None, "green"), (60, "yellow"), (70, "red"))),
        stat_panel("Outdoor Humidity", pos(12, hvac_y, 4, 4),
            ha_last("%", "lansing_st_weather_humidity"),
            unit="percent", decimals=0,
            thresh=thresholds((None, "green"), (70, "yellow"), (85, "red"))),
        stat_panel("Feels Like", pos(16, hvac_y, 4, 4),
            ha_last("°F", "lansing_st_weather_feelslike"),
            unit="fahrenheit", decimals=0,
            thresh=thresholds((None, "blue"), (40, "green"), (80, "yellow"), (90, "red"))),
        stat_panel("Fridge / Freezer", pos(20, hvac_y, 4, 4),
            f"""
from(bucket: "bartos-homeassistant")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "°F" and r._field == "value"
      and (r.entity_id == "chill_box_fridge_temperature" or r.entity_id == "chill_box_freezer_temperature"))
  |> last()
  |> map(fn: (r) => ({{r with _field: if r.entity_id == "chill_box_fridge_temperature" then "Fridge" else "Freezer"}}))
""",
            unit="fahrenheit", decimals=0,
            thresh=thresholds((None, "blue"), (0, "green"), (40, "yellow"), (45, "red"))),
    ]

    # ── Row 4: Power (h=7, y=17)
    panels.append(row_panel("⚡ Power", 17))
    power_y = 18
    panels += [
        gauge_panel("Total Power", pos(0, power_y, 5, 7),
            ha_last("W", "emporia_01_total_power"),
            unit="watt", min_val=0, max_val=5000, decimals=0,
            thresh=thresholds((None, "green"), (2000, "yellow"), (3500, "red"))),
        gauge_panel("Phase A", pos(5, power_y, 5, 7),
            ha_last("W", "emporia_01_phase_a_power"),
            unit="watt", min_val=0, max_val=3000, decimals=0,
            thresh=thresholds((None, "green"), (1500, "yellow"), (2500, "red"))),
        gauge_panel("Phase B", pos(10, power_y, 5, 7),
            ha_last("W", "emporia_01_phase_b_power"),
            unit="watt", min_val=0, max_val=3000, decimals=0,
            thresh=thresholds((None, "green"), (1500, "yellow"), (2500, "red"))),
        bar_gauge_panel("Circuit Breakdown", pos(15, power_y, 9, 7),
            f"""
from(bucket: "bartos-homeassistant")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "W" and r._field == "value" and r.entity_id =~ /^emporia_01_circuit_\\d+_power$/)
  |> last()
  |> filter(fn: (r) => r._value > 5)
  |> map(fn: (r) => ({{r with _field: r.entity_id}}))
  |> keep(columns: ["_value", "_field", "_time"])
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 10)
""",
            unit="watt", orient="horizontal", display_mode="gradient",
            thresh=thresholds((None, "green"), (1000, "yellow"), (2000, "red"))),
    ]

    # ── Row 5: Weather Station (h=4, y=25)
    panels.append(row_panel("🌤 Weather Station", 25))
    wx_y = 26
    panels += [
        stat_panel("Wind Speed", pos(0, wx_y, 4, 4),
            ha_last("mph", "lansing_st_weather_windspeed"),
            unit="velocitymph", decimals=0,
            thresh=thresholds((None, "green"), (15, "yellow"), (30, "red"))),
        stat_panel("Wind Gust", pos(4, wx_y, 4, 4),
            ha_last("mph", "lansing_st_weather_windgust"),
            unit="velocitymph", decimals=0,
            thresh=thresholds((None, "green"), (20, "yellow"), (40, "red"))),
        stat_panel("UV Index", pos(8, wx_y, 4, 4),
            ha_last("UV index", "lansing_st_weather_uv"),
            unit="none", decimals=0,
            thresh=thresholds((None, "green"), (3, "yellow"), (6, "orange"), (8, "red"))),
        stat_panel("Daily Rain", pos(12, wx_y, 4, 4),
            ha_last("in", "lansing_st_weather_dailyrain"),
            unit="lengthmi", decimals=2,
            thresh=thresholds((None, "blue"), (0.5, "green"), (2, "yellow"))),
        stat_panel("Solar Radiation", pos(16, wx_y, 4, 4),
            ha_last("W/m²", "lansing_st_weather_solarradiation"),
            unit="wm2", decimals=0,
            thresh=thresholds((None, "gray"), (100, "yellow"), (600, "orange"))),
        stat_panel("Barometer", pos(20, wx_y, 4, 4),
            ha_last("inHg", "lansing_st_weather_baromrel"),
            unit="pressurehg", decimals=2,
            thresh=thresholds((None, "blue"), (29.8, "green"), (30.2, "blue"))),
    ]

    d["panels"] = panels
    return d


# ══════════════════════════════════════════════════════════════════
# DASHBOARD 2: HOMELAB STATUS
# ══════════════════════════════════════════════════════════════════

def build_homelab_status():
    d = dashboard_base("tv-homelab-status", "🖥️ Homelab Status", tags=["tv", "homelab"])
    panels = []

    # ── Row 1: Fleet Summary (h=5, y=0)
    panels.append(row_panel("🔭 Fleet", 0))
    fleet_y = 1
    panels += [
        stat_panel("Nodes Reporting", pos(0, fleet_y, 6, 5),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "node_cpu" and r._field == "cpu_used_pct")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0, text_size=20,
            thresh=thresholds((None, "red"), (20, "yellow"), (28, "green"))),
        stat_panel("Active Alarms", pos(6, fleet_y, 6, 5),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "node_alarms" and r._field == "severity" and r._value > 0)
  |> filter(fn: (r) => r.node_name != "netdata-snmp-01")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0, text_size=20,
            thresh=thresholds((None, "green"), (1, "yellow"), (5, "red"))),
        gauge_panel("UPS Battery", pos(12, fleet_y, 6, 5),
            ha_last("%", "server_ups_battery_charge"),
            unit="percent", min_val=0, max_val=100, decimals=0,
            thresh=thresholds((None, "red"), (20, "yellow"), (50, "green"))),
        stat_panel("UPS Runtime", pos(18, fleet_y, 6, 5),
            ha_last("min", "server_ups_battery_runtime"),
            unit="m", decimals=0, text_size=20,
            thresh=thresholds((None, "red"), (5, "yellow"), (15, "green"))),
    ]

    # ── Row 2: UPS & TrueNAS (h=5, y=6)
    panels.append(row_panel("🔌 UPS & Storage", 6))
    ups_y = 7
    panels += [
        gauge_panel("UPS Load", pos(0, ups_y, 4, 5),
            ha_last("%", "server_ups_load"),
            unit="percent", min_val=0, max_val=100, decimals=0,
            thresh=thresholds((None, "green"), (60, "yellow"), (85, "red"))),
        stat_panel("UPS Input Voltage", pos(4, ups_y, 4, 5),
            ha_last("V", "server_ups_input_voltage"),
            unit="volt", decimals=0,
            thresh=thresholds((None, "red"), (110, "yellow"), (115, "green"), (125, "yellow"), (130, "red"))),
        stat_panel("TrueNAS Pool", pos(8, ups_y, 4, 5),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "truenas_pool" and r._field == "status")
  |> last()
""",
            unit="string", decimals=0,
            thresh=thresholds((None, "green")),
            mappings=[{
                "type": "value",
                "options": {
                    "ONLINE": {"text": "✅ ONLINE", "color": "green", "index": 0},
                    "DEGRADED": {"text": "⚠️ DEGRADED", "color": "yellow", "index": 1},
                    "FAULTED": {"text": "🚨 FAULTED", "color": "red", "index": 2},
                }
            }]),
        gauge_panel("Archive Pool Used", pos(12, ups_y, 4, 5),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "truenas_pool" and r._field == "used_pct")
  |> last()
""",
            unit="percent", min_val=0, max_val=100, decimals=1,
            thresh=thresholds((None, "green"), (75, "yellow"), (90, "red"))),
        stat_panel("Hottest Disk", pos(16, ups_y, 4, 5),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "truenas_disk_temp" and r._field == "temp_celsius")
  |> last()
  |> group()
  |> max()
""",
            unit="celsius", decimals=0,
            thresh=thresholds((None, "green"), (40, "yellow"), (50, "red"))),
        stat_panel("Disk Count", pos(20, ups_y, 4, 5),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "truenas_disk_temp" and r._field == "temp_celsius")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
    ]

    # ── Row 3: Top CPU & Memory (h=8, y=12)
    panels.append(row_panel("🔥 Resource Usage", 12))
    res_y = 13
    panels += [
        bar_gauge_panel("Top CPU Nodes (%)", pos(0, res_y, 12, 8),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "node_cpu" and r._field == "cpu_used_pct")
  |> last()
  |> map(fn: (r) => ({{r with _field: r.node_name}}))
  |> keep(columns: ["_value", "_field", "_time"])
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 10)
""",
            unit="percent", orient="horizontal", display_mode="gradient",
            thresh=thresholds((None, "green"), (50, "yellow"), (80, "orange"), (95, "red")),
            decimals=1),
        bar_gauge_panel("Top Memory Nodes (%)", pos(12, res_y, 12, 8),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "node_memory" and r._field == "ram_used_pct")
  |> last()
  |> map(fn: (r) => ({{r with _field: r.node_name}}))
  |> keep(columns: ["_value", "_field", "_time"])
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 10)
""",
            unit="percent", orient="horizontal", display_mode="gradient",
            thresh=thresholds((None, "green"), (75, "yellow"), (90, "orange"), (95, "red")),
            decimals=1),
    ]

    # ── Row 4: UniFi Devices (h=4, y=21)
    panels.append(row_panel("🌐 Network", 21))
    net_y = 22
    panels += [
        stat_panel("UniFi Clients", pos(0, net_y, 4, 4),
            f"""
from(bucket: "unifi")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "clients" and r._field == "bytes_r")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
        stat_panel("Wireless Clients", pos(4, net_y, 4, 4),
            f"""
from(bucket: "unifi")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "clients" and r._field == "bytes_r" and r.is_wired == "false")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
        stat_panel("Wired Clients", pos(8, net_y, 4, 4),
            f"""
from(bucket: "unifi")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "clients" and r._field == "bytes_r" and r.is_wired == "true")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
        timeseries_panel("Total Network Throughput", pos(12, net_y, 12, 4),
            [(f"""
from(bucket: "unifi")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "clients" and r._field == "bytes_r")
  |> sum()
  |> map(fn: (r) => ({{r with _value: r._value / 1000.0}}))
""", "kbps")],
            unit="kbps", fill_opacity=20),
    ]

    # ── Row 5: Active Alarms Table (h=7, y=26)
    panels.append(row_panel("⚠️ Active Alarms", 26))
    alm_y = 27
    panels.append(
        table_panel("Active Alarms (excl. netdata-snmp-01)", pos(0, alm_y, 24, 7),
            f"""
from(bucket: "netdata")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "node_alarms" and r._field == "summary")
  |> filter(fn: (r) => r.node_name != "netdata-snmp-01")
  |> last()
  |> keep(columns: ["node_name", "alarm_name", "_value", "_time"])
  |> rename(columns: {{node_name: "Node", alarm_name: "Alarm", _value: "Summary"}})
""",
        )
    )

    d["panels"] = panels
    return d


# ══════════════════════════════════════════════════════════════════
# DASHBOARD 3: MEDIA & NETWORK
# ══════════════════════════════════════════════════════════════════

def build_media_network():
    d = dashboard_base("tv-media-network", "🎬 Media & Network", tags=["tv", "media"])
    panels = []

    # ── Row 1: Plex Stats (h=5, y=0)
    panels.append(row_panel("🎬 Plex", 0))
    plex_y = 1
    panels += [
        stat_panel("Active Streams", pos(0, plex_y, 4, 5),
            f"""
from(bucket: "plex")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "plays_total")
  |> last()
  |> group()
  |> count()
""",
            unit="none", decimals=0, text_size=24,
            thresh=thresholds((None, "gray"), (1, "green")),
            no_value="0"),
        stat_panel("Movies", pos(4, plex_y, 4, 5),
            f"""
from(bucket: "plex")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "plex_media_movies")
  |> last()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
        stat_panel("TV Episodes", pos(8, plex_y, 4, 5),
            f"""
from(bucket: "plex")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "plex_media_episodes")
  |> last()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
        stat_panel("Music Artists", pos(12, plex_y, 4, 5),
            f"""
from(bucket: "plex")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "plex_media_music")
  |> last()
""",
            unit="none", decimals=0,
            thresh=thresholds((None, "blue"))),
        stat_panel("Plex CPU", pos(16, plex_y, 4, 5),
            f"""
from(bucket: "plex")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "host_cpu_util")
  |> last()
""",
            unit="percent", decimals=1,
            thresh=thresholds((None, "green"), (50, "yellow"), (80, "red"))),
        stat_panel("Plex Memory", pos(20, plex_y, 4, 5),
            f"""
from(bucket: "plex")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "host_mem_util")
  |> last()
""",
            unit="percent", decimals=1,
            thresh=thresholds((None, "green"), (75, "yellow"), (90, "red"))),
    ]

    # ── Row 2: Library Breakdown (h=7, y=6)
    panels.append(row_panel("📚 Libraries", 6))
    lib_y = 7
    panels += [
        bar_gauge_panel("Storage by Library (TB)", pos(0, lib_y, 12, 7),
            f"""
from(bucket: "plex")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "library_storage_total")
  |> last()
  |> map(fn: (r) => ({{r with _value: r._value / 1099511627776.0, _field: r.library}}))
  |> keep(columns: ["_time", "_value", "_field"])
""",
            unit="decbytes", orient="horizontal", display_mode="gradient",
            thresh=thresholds((None, "blue"), (5, "green"), (15, "yellow")),
            decimals=1),
        bar_gauge_panel("Duration by Library (hrs)", pos(12, lib_y, 12, 7),
            f"""
from(bucket: "plex")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "library_duration_total")
  |> last()
  |> map(fn: (r) => ({{r with _value: r._value / 3600000.0, _field: r.library}}))
  |> keep(columns: ["_time", "_value", "_field"])
""",
            unit="h", orient="horizontal", display_mode="gradient",
            thresh=thresholds((None, "blue"), (100, "green"), (1000, "yellow")),
            decimals=0),
    ]

    # ── Row 3: Bandwidth & Activity (h=6, y=14)
    panels.append(row_panel("📡 Activity", 14))
    act_y = 15
    panels += [
        timeseries_panel("Plex Stream Bandwidth (MB/s)", pos(0, act_y, 12, 6),
            [(f"""
from(bucket: "plex")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "transmit_bytes_total")
  |> derivative(unit: 1s, nonNegative: true)
  |> map(fn: (r) => ({{r with _value: r._value / 1048576.0}}))
""", "Plex Bandwidth")],
            unit="MBs", fill_opacity=25),
        timeseries_panel("Plex Host CPU & Memory", pos(12, act_y, 12, 6),
            [
                (f"""
from(bucket: "plex")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "host_cpu_util")
""", "CPU %"),
                (f"""
from(bucket: "plex")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "host_mem_util")
""", "Memory %"),
            ],
            unit="percent", fill_opacity=15),
    ]

    # ── Row 4: Active Sessions (h=6, y=21)
    panels.append(row_panel("▶️ Active Sessions", 21))
    sess_y = 22
    panels.append(
        table_panel("Now Playing", pos(0, sess_y, 24, 6),
            f"""
from(bucket: "plex")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "prometheus" and r._field == "plays_total")
  |> last()
  |> keep(columns: ["user", "title", "child_title", "grandchild_title", "stream_type", "stream_resolution", "device", "media_type"])
""",
        )
    )

    # ── Row 5: Top UniFi Clients (h=6, y=28)
    panels.append(row_panel("📶 Top Clients", 28))
    clients_y = 29
    panels.append(
        bar_gauge_panel("Top 10 Clients by Bandwidth (kbps)", pos(0, clients_y, 24, 6),
            f"""
from(bucket: "unifi")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "clients" and r._field == "bytes_r")
  |> last()
  |> map(fn: (r) => ({{r with _field: if r.name != "" then r.name else if r.hostname != "" then r.hostname else r.mac}}))
  |> keep(columns: ["_value", "_field", "_time"])
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 10)
""",
            unit="kbps", orient="horizontal", display_mode="gradient",
            thresh=thresholds((None, "blue"), (1000, "green"), (10000, "yellow"), (50000, "red")),
            decimals=0),
    )

    d["panels"] = panels
    return d


# ══════════════════════════════════════════════════════════════════
# Push to Grafana
# ══════════════════════════════════════════════════════════════════

def push_dashboard(dash, dry_run=False):
    payload = {"dashboard": dash, "overwrite": True, "folderId": 0}
    body = json.dumps(payload).encode("utf-8")
    if dry_run:
        print(f"[DRY-RUN] Would push: {dash['uid']} — {dash['title']}")
        # Write JSON for inspection
        fname = f"/tmp/{dash['uid']}.json"
        with open(fname, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"          Written to {fname}")
        return True

    import base64
    creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{GRAFANA_URL}/api/dashboards/db",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {creds}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            print(f"  ✅ {dash['title']} → uid={result.get('uid')} url={result.get('url')}")
            return result
    except urllib.error.HTTPError as e:
        body_err = e.read().decode()
        print(f"  ❌ {dash['title']} failed: HTTP {e.code} — {body_err}")
        return None


def create_playlist(dash_uids, dry_run=False):
    """Create or overwrite the TV playlist."""
    items = [{"type": "dashboard_by_uid", "value": uid} for uid in dash_uids]
    payload = {
        "name": "🏠 Home TV Playlist",
        "interval": "45s",
        "items": items,
    }
    body = json.dumps(payload).encode("utf-8")
    if dry_run:
        print(f"[DRY-RUN] Would create playlist with {len(items)} dashboards @ 45s")
        return True

    import base64
    creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{GRAFANA_URL}/api/playlists",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {creds}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            pid = result.get("id") or result.get("uid", "?")
            print(f"  ✅ Playlist created — id={pid}")
            print(f"     Kiosk URL: {GRAFANA_URL}/playlists/play/{pid}?kiosk")
            return result
    except urllib.error.HTTPError as e:
        body_err = e.read().decode()
        print(f"  ❌ Playlist failed: HTTP {e.code} — {body_err}")
        return None


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    dashboards = [
        build_home_status(),
        build_homelab_status(),
        build_media_network(),
    ]

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Pushing {len(dashboards)} TV dashboards to Grafana...\n")
    results = []
    for dash in dashboards:
        r = push_dashboard(dash, dry_run=dry_run)
        results.append(r)

    print("\nCreating playlist...")
    uids = [d["uid"] for d in dashboards]
    create_playlist(uids, dry_run=dry_run)

    print(f"\nDone. {'(dry run)' if dry_run else ''}")
    if not dry_run:
        print(f"\nDashboard URLs:")
        for d in dashboards:
            print(f"  {d['title']:30s} → {GRAFANA_URL}/d/{d['uid']}?kiosk")
