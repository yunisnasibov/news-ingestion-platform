#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# telegram_backfill.sh — Bütün Telegram kanallarının TAM tarixçəsini çəkir.
#
# Hər kanalın ən qədim mesajına qədər geriyə gedir. Bitdikdən sonra çıxır.
# Kaldığı yerdən davam edir (upsert sayəsində təkrarlanma olmaz).
#
# İstifadə:
#   ./telegram_backfill.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
    set -a; source .env; set +a
fi
export PYTHONPATH="$SCRIPT_DIR:$SCRIPT_DIR/src"

PYTHON=".venv/bin/python"
if [ ! -f "$PYTHON" ]; then PYTHON="python3"; fi

echo "=== Telegram Full Backfill Başladı ==="
echo "Bütün kanalların TAM tarixçəsi çəkiləcək."
echo "Ctrl+C ilə dayandıra bilərsiniz."
echo ""

"$PYTHON" -m news_ingestor.cli backfill-all-telegram

echo ""
echo "=== Telegram Full Backfill Tamamlandı ==="
