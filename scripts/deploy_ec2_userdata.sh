#!/usr/bin/env bash
# ==============================================================================
# Bootstrap & Systemd Service Setup for EC2 (Amazon Linux 2023 or Ubuntu 22/24)
# Prerequisite: Launch EC2 t3.small with IAM Instance Profile `LabInstanceProfile`
#               and Security Group allowing inbound TCP 22 (SSH) and TCP 8000 (API).
#
# Run on EC2 inside the cloned project folder after creating `.env`:
#   chmod +x scripts/deploy_ec2_userdata.sh
#   sudo ./scripts/deploy_ec2_userdata.sh
# ==============================================================================
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="${SUDO_USER:-$(whoami)}"

echo "[1/4] Configuring swap (for t3.micro 1 GiB RAM safety) & installing Python 3..."
if [ ! -f /swapfile ]; then
    dd if=/dev/zero of=/swapfile bs=1M count=1024 status=none || true
    chmod 600 /swapfile || true
    mkswap /swapfile >/dev/null 2>&1 && swapon /swapfile || true
fi

if command -v dnf >/dev/null 2>&1; then
    dnf install -y python3.11 python3.11-pip git unzip
    PY_BIN="python3.11"
elif command -v apt-get >/dev/null 2>&1; then
    apt-get update -y
    apt-get install -y python3 python3-venv python3-pip git unzip
    PY_BIN="python3"
else
    PY_BIN="python3"
fi

echo "[2/4] Setting up virtual environment in ${APP_DIR}/.venv..."
sudo -u "${RUN_USER}" "${PY_BIN}" -m venv "${APP_DIR}/.venv"
sudo -u "${RUN_USER}" "${APP_DIR}/.venv/bin/pip" install --upgrade pip
sudo -u "${RUN_USER}" "${APP_DIR}/.venv/bin/pip" install -r "${APP_DIR}/requirements.txt"

if [ ! -f "${APP_DIR}/.env" ]; then
    echo "[WARN] .env not found! Copying .env.example -> .env (remember to edit S3_BUCKET!)"
    sudo -u "${RUN_USER}" cp "${APP_DIR}/.env.example" "${APP_DIR}/.env"
fi

echo "[3/4] Creating systemd unit /etc/systemd/system/airline-backend.service..."
cat >/etc/systemd/system/airline-backend.service <<EOF
[Unit]
Description=Airline Reservation System Backend + Kinesis Consumer (Parcial 3)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${APP_DIR}/.venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers 1
Restart=always
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

echo "[4/4] Enabling and starting airline-backend.service..."
systemctl daemon-reload
systemctl enable airline-backend.service
systemctl restart airline-backend.service
sleep 2
systemctl status airline-backend.service --no-pager

echo ""
echo "Backend running on port 8000! Test with: curl -s http://127.0.0.1:8000/health"
