#!/usr/bin/env bash
# scripts/cron_daily.sh - Daily automated maintenance cycle for Korean Sunrise
# Designed for cron execution on Linux/VPS servers (e.g. at 00:00 UTC)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

if [ -d "${PROJECT_ROOT}/venv" ]; then
    source "${PROJECT_ROOT}/venv/bin/activate"
elif [ -d "/root/venv" ]; then
    source "/root/venv/bin/activate"
fi

mkdir -p "${PROJECT_ROOT}/logs"

TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
echo "================================================================================"
echo "[CRON] Starting Korean Sunrise Daily Lifecycle Pipeline at ${TIMESTAMP}"
echo "================================================================================"

python3 tools/run_daily_pipeline.py "$@"

EXIT_CODE=$?
echo "[CRON] Daily Lifecycle finished with exit code ${EXIT_CODE} at $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
exit ${EXIT_CODE}
