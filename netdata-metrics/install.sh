#!/bin/bash
# Install netdata-metrics as a systemd user service on openclaw.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
VENV="$HOME/.local/venvs/netdata-metrics"

echo "Installing netdata-metrics..."

# Create venv + install deps
python3 -m venv "$VENV"
"$VENV/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
echo "✅ Dependencies installed"

# Write systemd unit
mkdir -p "$SERVICE_DIR"
cat > "$SERVICE_DIR/netdata-metrics.service" << EOF
[Unit]
Description=Netdata Metrics Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$VENV/bin/python -u $SCRIPT_DIR/collector.py
Restart=always
RestartSec=10
Environment=NETDATA_API=${NETDATA_API}
Environment=NETDATA_SPACE_ID=${NETDATA_SPACE_ID}
Environment=NETDATA_ROOM_ID=${NETDATA_ROOM_ID}
Environment=INFLUXDB_URL=${INFLUXDB_URL}
Environment=INFLUXDB_TOKEN=${INFLUXDB_TOKEN}
Environment=INFLUXDB_ORG=${INFLUXDB_ORG}
Environment=INFLUXDB_BUCKET=${INFLUXDB_BUCKET}
Environment=POLL_INTERVAL=${POLL_INTERVAL:-60}
Environment=MAX_WORKERS=${MAX_WORKERS:-8}
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now netdata-metrics.service
echo "✅ Service installed and started"
echo ""
echo "Logs: journalctl --user -u netdata-metrics -f"
