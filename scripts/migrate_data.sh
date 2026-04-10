#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# migrate_data.sh — Mevcut verileri yeni birleşik PostgreSQL'e taşır
#
# Kullanım:
#   chmod +x scripts/migrate_data.sh
#   ./scripts/migrate_data.sh
#
# Ön koşullar:
#   1. docker compose up -d postgres (yeni DB hazır olmalı)
#   2. Eski DB'ler erişilebilir olmalı
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Renk kodları ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Yeni (hedef) DB bağlantısı — Docker postgres, dış port 5433 ──
NEW_HOST="127.0.0.1"
NEW_PORT="5433"
NEW_DB="news_ingestor"
NEW_USER="news_ingestor"
export PGPASSWORD="news_ingestor"

# ──────────────────────────────────────────────────────────────────────────────
# 1) Web Scraper verileri: sonxeber_scraper → news_ingestor.articles
# ──────────────────────────────────────────────────────────────────────────────
migrate_web_scraper() {
    info "Web scraper verilerini taşıyorum..."

    # Eski DB bilgileri — kullanıcının yerel PostgreSQL'i
    local OLD_WS_HOST="${SONXEBER_OLD_HOST:-127.0.0.1}"
    local OLD_WS_PORT="${SONXEBER_OLD_PORT:-5432}"
    local OLD_WS_DB="${SONXEBER_OLD_DB:-sonxeber_scraper}"
    local OLD_WS_USER="${SONXEBER_OLD_USER:-$(whoami)}"

    # Eski DB'de articles tablosu var mı kontrol et
    local TABLE_EXISTS
    TABLE_EXISTS=$(PGPASSWORD="" psql -h "$OLD_WS_HOST" -p "$OLD_WS_PORT" -U "$OLD_WS_USER" -d "$OLD_WS_DB" -tAc \
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='articles')" 2>/dev/null || echo "f")

    if [ "$TABLE_EXISTS" != "t" ]; then
        warn "Eski web scraper DB'de ($OLD_WS_DB) articles tablosu bulunamadı, atlıyorum."
        return 0
    fi

    local OLD_COUNT
    OLD_COUNT=$(PGPASSWORD="" psql -h "$OLD_WS_HOST" -p "$OLD_WS_PORT" -U "$OLD_WS_USER" -d "$OLD_WS_DB" -tAc \
        "SELECT COUNT(*) FROM articles" 2>/dev/null || echo "0")
    info "Eski web scraper DB'de $OLD_COUNT article bulundu."

    if [ "$OLD_COUNT" = "0" ]; then
        warn "Eski DB boş, atlıyorum."
        return 0
    fi

    # Hedef DB'de articles tablosunu oluştur (yoksa)
    psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -c "
        CREATE TABLE IF NOT EXISTS articles (
            id BIGSERIAL PRIMARY KEY,
            source_name TEXT NOT NULL,
            source_article_id BIGINT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            content_text TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT 'uncategorized',
            article_url TEXT NOT NULL,
            image_url TEXT NOT NULL DEFAULT '',
            published_at TEXT NOT NULL DEFAULT '',
            fetched_at TIMESTAMPTZ NOT NULL,
            UNIQUE(source_name, source_article_id)
        );
        CREATE INDEX IF NOT EXISTS idx_articles_source_article_url
        ON articles(source_name, article_url);
    " 2>/dev/null

    # pg_dump ile veri aktar
    info "pg_dump ile articles verilerini aktarıyorum..."
    PGPASSWORD="" pg_dump -h "$OLD_WS_HOST" -p "$OLD_WS_PORT" -U "$OLD_WS_USER" -d "$OLD_WS_DB" \
        --table=articles --data-only --no-owner --no-privileges \
        --on-conflict-do-nothing 2>/dev/null | \
    psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -q 2>/dev/null || {
        warn "pg_dump aktar başarısız, COPY ile deniyorum..."
        PGPASSWORD="" psql -h "$OLD_WS_HOST" -p "$OLD_WS_PORT" -U "$OLD_WS_USER" -d "$OLD_WS_DB" \
            -c "\COPY articles TO STDOUT WITH CSV HEADER" 2>/dev/null | \
        psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" \
            -c "\COPY articles FROM STDIN WITH CSV HEADER" 2>/dev/null
    }

    # Seq düzelt (PK hatası almamak için)
    info "Sequence (id) güncelleniyor..."
    psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" \
        -c "SELECT setval('articles_id_seq', (SELECT MAX(id) FROM articles))" > /dev/null

    local NEW_COUNT
    NEW_COUNT=$(psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -tAc \
        "SELECT COUNT(*) FROM articles" 2>/dev/null || echo "0")
    info "Yeni DB'de articles: $NEW_COUNT"
}

# ──────────────────────────────────────────────────────────────────────────────
# 2) Telegram verileri: eski news_ingestor → yeni news_ingestor.news
# ──────────────────────────────────────────────────────────────────────────────
migrate_telegram() {
    info "Telegram verilerini taşıyorum..."

    local OLD_TG_HOST="${TELEGRAM_OLD_HOST:-127.0.0.1}"
    local OLD_TG_PORT="${TELEGRAM_OLD_PORT:-5433}"
    local OLD_TG_DB="${TELEGRAM_OLD_DB:-news_ingestor}"
    local OLD_TG_USER="${TELEGRAM_OLD_USER:-news_ingestor}"
    local OLD_TG_PASS="${TELEGRAM_OLD_PASS:-news_ingestor}"

    # Eski DB'de news tablosu var mı kontrol et
    local TABLE_EXISTS
    TABLE_EXISTS=$(PGPASSWORD="$OLD_TG_PASS" psql -h "$OLD_TG_HOST" -p "$OLD_TG_PORT" -U "$OLD_TG_USER" -d "$OLD_TG_DB" -tAc \
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='news')" 2>/dev/null || echo "f")

    if [ "$TABLE_EXISTS" != "t" ]; then
        warn "Eski telegram DB'de news tablosu bulunamadı, atlıyorum."
        return 0
    fi

    local OLD_COUNT
    OLD_COUNT=$(PGPASSWORD="$OLD_TG_PASS" psql -h "$OLD_TG_HOST" -p "$OLD_TG_PORT" -U "$OLD_TG_USER" -d "$OLD_TG_DB" -tAc \
        "SELECT COUNT(*) FROM news" 2>/dev/null || echo "0")
    info "Eski telegram DB'de $OLD_COUNT news row bulundu."

    if [ "$OLD_COUNT" = "0" ]; then
        warn "Eski DB boş, atlıyorum."
        return 0
    fi

    # pg_dump ile veri aktar
    info "pg_dump ile news verilerini aktarıyorum..."
    PGPASSWORD="$OLD_TG_PASS" pg_dump -h "$OLD_TG_HOST" -p "$OLD_TG_PORT" -U "$OLD_TG_USER" -d "$OLD_TG_DB" \
        --table=news --data-only --no-owner --no-privileges 2>/dev/null | \
    psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -q 2>/dev/null

    local NEW_COUNT
    NEW_COUNT=$(psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -tAc \
        "SELECT COUNT(*) FROM news" 2>/dev/null || echo "0")
    info "Yeni DB'de news: $NEW_COUNT"
}

# ──────────────────────────────────────────────────────────────────────────────
# 3) Doğrulama
# ──────────────────────────────────────────────────────────────────────────────
verify() {
    info "═══════════ DOĞRULAMA ═══════════"
    echo ""

    local ARTICLES_COUNT NEWS_COUNT
    ARTICLES_COUNT=$(psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -tAc \
        "SELECT COUNT(*) FROM articles" 2>/dev/null || echo "0")
    NEWS_COUNT=$(psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -tAc \
        "SELECT COUNT(*) FROM news" 2>/dev/null || echo "0")

    info "articles (web scraper): $ARTICLES_COUNT"
    info "news (telegram):        $NEWS_COUNT"
    echo ""

    # Tablo izolasyonunu doğrula
    local TABLES
    TABLES=$(psql -h "$NEW_HOST" -p "$NEW_PORT" -U "$NEW_USER" -d "$NEW_DB" -tAc \
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name" 2>/dev/null)
    info "Tablolar:"
    echo "$TABLES" | while read -r t; do
        [ -n "$t" ] && echo "  ✓ $t"
    done
    echo ""
    info "═══════════ TAMAMLANDI ═══════════"
}

# ──────────────────────────────────────────────────────────────────────────────
main() {
    info "════════════════════════════════════════════════════"
    info "  News Platform — Veri Göçü Başlıyor"
    info "════════════════════════════════════════════════════"
    echo ""
    migrate_web_scraper
    echo ""
    migrate_telegram
    echo ""
    verify
}

main "$@"
