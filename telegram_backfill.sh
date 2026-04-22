#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
    set -a; source .env; set +a
fi
export PYTHONPATH="$SCRIPT_DIR:$SCRIPT_DIR/src"

PYTHON=".venv/bin/python"
if [ ! -f "$PYTHON" ]; then PYTHON="python3"; fi

if [ "${ALLOW_PARALLEL_TELEGRAM_BACKFILL:-0}" != "1" ] && docker compose ps telegram-worker 2>/dev/null | grep -q " Up "; then
    echo "telegram_backfill_refused managed_telegram_worker_running=true"
    echo "telegram-worker service artıq historical -> live axınını özü idarə edir."
    echo "Manual telegram_backfill.sh parallel işə salınmır ki, duplicate və checkpoint qarışıqlığı yaranmasın."
    echo "Lazımdırsa əvvəl 'docker compose stop telegram-worker' edin və sonra ALLOW_PARALLEL_TELEGRAM_BACKFILL=1 ilə manual run edin."
    exit 2
fi

echo "=== Telegram Full Backfill Başladı ==="
echo "Bütün kanalların TAM tarixçəsi çəkiləcək."
echo "Ctrl+C ilə dayandıra bilərsiniz."
echo ""

"$PYTHON" -m news_ingestor.cli backfill-all-telegram

echo ""
echo "=== Telegram Full Backfill Tamamlandı ==="
