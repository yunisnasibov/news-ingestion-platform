#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

service_running() {
  docker compose ps web-scraper 2>/dev/null | grep -q "web-scraper" && \
    docker compose ps web-scraper 2>/dev/null | grep -q " Up "
}

legacy_run_container() {
  docker ps --format '{{.Names}}' 2>/dev/null | grep '^new_ingestor-web-scraper-run-' | head -n 1 || true
}

if [ "${ALLOW_PARALLEL_BACKFILL:-0}" != "1" ]; then
  if service_running; then
    echo "historical_backfill_refused managed_web_scraper_service_running=true"
    echo "web-scraper service artıq historical -> live axınını özü idarə edir."
    echo "Manual historical_backfill.sh parallel işə salınmır ki, lock konflikti yaranmasın."
    echo "Lazımdırsa əvvəl 'docker compose stop web-scraper' edin və sonra ALLOW_PARALLEL_BACKFILL=1 ilə manual run edin."
    exit 2
  fi

  RUN_CONTAINER="$(legacy_run_container)"
  if [ -n "$RUN_CONTAINER" ]; then
    echo "historical_backfill_refused legacy_run_container=${RUN_CONTAINER}"
    echo "Eyni anda birdən çox historical runner işləməsin deyə çıxılır."
    exit 2
  fi
fi

mkdir -p data/backfill_logs

if [ "$#" -gt 0 ]; then
  SOURCES=("$@")
else
  SOURCES=(
    azertag.az
    sonxeber.az
    azerbaijan.az
    ikisahil.az
    apa.az
    yenixeber.az
    teleqraf.az
    azxeber.com
    siyasetinfo.az
    metbuat.az
    1news.az
    sia.az
    xeberler.az
    islamazeri.com
    islam.az
    axar.az
    milli.az
    report.az
    iqtisadiyyat.az
    oxu.az
  )
fi

MAX_PAGES="${BACKFILL_MAX_PAGES:-0}"
STOP_EMPTY_PAGES="${BACKFILL_STOP_EMPTY_PAGES:-3}"
RETRY_ATTEMPTS="${BACKFILL_RETRY_ATTEMPTS:-5}"
RETRY_SLEEP_SECONDS="${BACKFILL_RETRY_SLEEP_SECONDS:-30}"
WAIT_FOR_LIVE_SECONDS="${BACKFILL_WAIT_FOR_LIVE_SECONDS:-600}"

for source in "${SOURCES[@]}"; do
  log_path="data/backfill_logs/${source}.log"
  attempt=1
  while true; do
    echo "[historical] $(date --iso-8601=seconds) source=${source} attempt=${attempt}/${RETRY_ATTEMPTS} max_pages=${MAX_PAGES} stop_empty=${STOP_EMPTY_PAGES} wait_for_live=${WAIT_FOR_LIVE_SECONDS}" | tee -a "$log_path"
    if docker compose run --rm -e PYTHONUNBUFFERED=1 web-scraper \
      python -m sonxeber_scraper backfill \
      --source "$source" \
      --max-pages "$MAX_PAGES" \
      --stop-empty-pages "$STOP_EMPTY_PAGES" \
      --wait-for-live-seconds "$WAIT_FOR_LIVE_SECONDS" | tee -a "$log_path"; then
      echo "[historical] completed source=${source}" | tee -a "$log_path"
      break
    fi

    if [ "$attempt" -ge "$RETRY_ATTEMPTS" ]; then
      echo "[historical] source=${source} failed after ${RETRY_ATTEMPTS} attempts" | tee -a "$log_path"
      exit 1
    fi

    echo "[historical] source=${source} failed attempt=${attempt}; sleeping ${RETRY_SLEEP_SECONDS}s before retry" | tee -a "$log_path"
    sleep "$RETRY_SLEEP_SECONDS"
    attempt=$((attempt + 1))
  done
done
