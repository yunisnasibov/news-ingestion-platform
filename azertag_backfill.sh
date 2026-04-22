#!/usr/bin/env bash
# azertag_continuous_backfill.sh — Azertag backfill'i hiç durdurmadan çalıştırır.
# Bir oturum bitince DB'deki en düşük ID'den devam eder.
# Ctrl+C ile durdurabilirsiniz.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
    set -a; source .env; set +a
fi
export PYTHONPATH="$SCRIPT_DIR:$SCRIPT_DIR/src"

PYTHON=".venv/bin/python"
if [ ! -f "$PYTHON" ]; then PYTHON="python3"; fi

echo "=== Azertag Sürekli Backfill Başlatıldı ==="
echo "Durdurmak için Ctrl+C basın."
echo ""

round=0
while true; do
    round=$((round + 1))
    echo "[Round $round] $(date '+%H:%M:%S') — backfill başlıyor..."
    # stop-empty-pages=200 → 200*5=1000 boş batch tolere edilir (~50,000 boş ID)
    "$PYTHON" main.py backfill \
        --source azertag.az \
        --stop-empty-pages 200 \
        || true
    echo "[Round $round] $(date '+%H:%M:%S') — oturum bitti, 2sn bekleyip devam ediliyor..."
    sleep 2
done
