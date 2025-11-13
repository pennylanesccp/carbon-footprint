#!/usr/bin/env bash
# run_routes_test.sh
# Small helper to test routes_generator caching behaviour.

set -euo pipefail

# Adjust if needed
PYTHON_BIN="python"
SCRIPT_PATH="scripts/routes_generator.py"
DB_PATH="data/routes.db"          # or leave empty to use default
TABLE_NAME="routes"               # or whatever DEFAULT_TABLE is

ORIGIN_RAW="Av. Luciano Gualberto"   # try a "short" version first

DESTINIES=(
    "Acrelandia, R., Brazil"
    "Assis Brasil, R., Brazil"
    "Brasileia, R., Brazil"
    "Bujari, R., Brazil"
    "Capixaba, R., Brazil"
)

for DEST in "${DESTINIES[@]}"; do
    echo "────────────────────────────────────────────────────────────"
    echo "Running route for:"
    echo "  origin : ${ORIGIN_RAW}"
    echo "  destiny: ${DEST}"
    echo

    ${PYTHON_BIN} "${SCRIPT_PATH}" \
        --origin  "${ORIGIN_RAW}" \
        --destiny "${DEST}" \
        --db-path "${DB_PATH}" \
        --table   "${TABLE_NAME}" \
        --log-level "DEBUG"

    echo
done
