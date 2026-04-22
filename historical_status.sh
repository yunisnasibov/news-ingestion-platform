#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RUNNER_PID=$(ps -ef | grep "./historical_backfill.sh" | grep -v grep | awk '{print $2}' | head -n 1 || true)
RUN_CONTAINER=$(docker ps --format '{{.Names}}' | grep '^new_ingestor-web-scraper-run-' | head -n 1 || true)

if [ -n "${RUNNER_PID:-}" ]; then
  echo "manual_runner_pid=${RUNNER_PID}"
else
  echo "manual_runner_pid=stopped"
fi

if [ -n "${RUN_CONTAINER:-}" ]; then
  echo "legacy_run_container=${RUN_CONTAINER}"
else
  echo "legacy_run_container=none"
fi

echo
 echo "== docker =="
docker compose ps web-scraper postgres

echo
 echo "== state =="
if compgen -G "data/backfill_state/*.json" > /dev/null; then
  for f in data/backfill_state/*.json; do
    echo "--- $(basename "$f") ---"
    sed -n "1,40p" "$f"
  done
else
  echo "no_state_files"
fi

echo
 echo "== recent log =="
if [ -f data/backfill_logs/historical.runner.log ]; then
  tail -n 40 data/backfill_logs/historical.runner.log
else
  echo "no_runner_log"
fi
