#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# run.sh — Yerel ortamda her iki scraper'i çalıştırmak için wrapper script
#
# .env dosyasını otomatik yükler ve PYTHONPATH'i ayarlar.
#
# Kullanım:
#   ./run.sh web stats                           # Web scraper statistika
#   ./run.sh web sync-once --source oxu.az       # Tek site bir döngü
#   ./run.sh web poll                            # Web scraper sürekli
#   ./run.sh telegram init-db                    # Telegram DB init
#   ./run.sh telegram run-telegram-worker        # Telegram worker
#   ./run.sh telegram source-status              # Telegram status
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# .env dosyasını yükle
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export PYTHONPATH="$SCRIPT_DIR:$SCRIPT_DIR/src"

# venv varsa aktifleştir
if [ -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
else
    PYTHON="python3"
fi

SUBSYSTEM="${1:-help}"
shift || true

case "$SUBSYSTEM" in
    web)
        exec "$PYTHON" main.py "$@"
        ;;
    telegram)
        exec "$PYTHON" -m news_ingestor.cli "$@"
        ;;
    help|--help|-h|"")
        echo "Kullanım: ./run.sh <web|telegram> [args...]"
        echo ""
        echo "  web       Web scraper (sonxeber_scraper)"
        echo "  telegram  Telegram scraper (news_ingestor)"
        echo ""
        echo "Örnəklər:"
        echo "  ./run.sh web stats"
        echo "  ./run.sh web sync-once --source oxu.az"
        echo "  ./run.sh web poll"
        echo "  ./run.sh telegram init-db"
        echo "  ./run.sh telegram run-telegram-worker"
        echo "  ./run.sh telegram source-status"
        ;;
    *)
        echo "Bilinməyən alt-sistem: $SUBSYSTEM"
        echo "Kullanım: ./run.sh <web|telegram> [args...]"
        exit 1
        ;;
esac
