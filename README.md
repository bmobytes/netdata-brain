# netdata-brain 🧠

Homelab network monitoring stack for Bartos Cloud.

Polls the Netdata Cloud API, writes metrics to InfluxDB, serves a live status page, and posts Discord alerts for node state changes and critical alerts.

## Services

| Service | Port | Purpose |
|---------|------|---------|
| `netdata-brain` | — | Poller: Netdata API → InfluxDB + Discord alerts |
| `status-page` | 8080 | Live fleet status dashboard |
| `influxdb` | 8086 | Time-series metrics storage |
| `grafana` | 3001 | Dashboards |

## Stack

- Python 3.12, Flask, influxdb-client, requests
- InfluxDB 2.7
- Grafana
- Traefik v3 (shared reverse proxy, in `traefik/`)

## Setup

```bash
# 1. Create the shared Docker network (once per host)
docker network create bmo-net

# 2. Start Traefik (shared reverse proxy)
cd traefik && docker compose up -d && cd ..

# 3. Configure
cp .env.example .env
# Edit .env with your tokens and passwords

# 4. Build and deploy
docker compose up -d --build

# 5. Verify
docker compose ps
docker compose logs -f netdata-brain
```

## What gets alerted

- Node goes offline (stale/unreachable)
- Node comes back online
- New critical alerts on any node
- Critical alerts cleared
- Daily digest at 9am UTC

## URLs (via Traefik)

- `http://status.bmo.lab.bartos.media` — status page
- `http://grafana.bmo.lab.bartos.media` — Grafana
- `http://influxdb.bmo.lab.bartos.media` — InfluxDB
- `http://<host>:8090` — Traefik dashboard
