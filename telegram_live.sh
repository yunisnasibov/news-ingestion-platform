#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# telegram_live.sh — Əvvəlcə backfill-i çalışdırır, bitdikdən sonra
# avtomatik olaraq canlı (live) Telegram worker-ə keçir.
#
# İstifadə:
#   ./telegram_live.sh
#
# Bu script:
#   1) telegram_backfill.sh ilə eyni işi görər (tam tarixçə çəkir)
#   2) Backfill bitdikdən sonra run-telegram-worker ilə canlı dinləməyə başlayar
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

echo "╔══════════════════════════════════════════════════╗"
echo "║  MƏRHƏLƏ 1: Telegram Full Backfill               ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

"$PYTHON" -m news_ingestor.cli backfill-all-telegram

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  MƏRHƏLƏ 2: Canlı (Live) Telegram Worker         ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Backfill tamamlandı. İndi canlı mesaj dinləməyə keçilir..."
echo ""

"$PYTHON" -m news_ingestor.cli run-telegram-worker
