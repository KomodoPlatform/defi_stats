#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/pg_restore.sh -f /path/to/backup.dump [options]

Options:
  -f PATH   Dump file created by pg_backup.sh (required)
  -e PATH   Path to the .env file (default: <repo>/api/.env)
  -c NAME   Docker Compose service/container name (default: pgsqldb)
  -p PATH   Compose project directory (default: repo root)
  -h        Show this help message

The script restores the provided dump into the Postgres database defined in
api/.env by piping the file into `pg_restore` inside the pgsqldb container.
Ensure the database service is running before invoking the script.
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/api/.env}"
PG_CONTAINER="${PG_CONTAINER:-pgsqldb}"
PROJECT_DIR="${PROJECT_DIR:-${REPO_ROOT}}"
DUMP_FILE=""

while getopts "f:e:c:p:h" opt; do
    case "${opt}" in
        f) DUMP_FILE="$(realpath "${OPTARG}")" ;;
        e) ENV_FILE="$(realpath "${OPTARG}")" ;;
        c) PG_CONTAINER="${OPTARG}" ;;
        p) PROJECT_DIR="$(realpath "${OPTARG}")" ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

if [[ -z "${DUMP_FILE}" ]]; then
    echo "Dump file is required. Use -f /path/to/backup.dump" >&2
    exit 1
fi

if [[ ! -f "${DUMP_FILE}" ]]; then
    echo "Dump file not found: ${DUMP_FILE}" >&2
    exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Env file not found: ${ENV_FILE}" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

DB_USER="${POSTGRES_USERNAME:-${POSTGRES_USER:-}}"
DB_NAME="${POSTGRES_DATABASE:-${POSTGRES_DB:-}}"
DB_PASSWORD="${POSTGRES_APP_PASSWORD:-${POSTGRES_PASSWORD:-}}"

if [[ -z "${DB_USER}" || -z "${DB_NAME}" || -z "${DB_PASSWORD}" ]]; then
    echo "POSTGRES_USERNAME/POSTGRES_DATABASE/POSTGRES_PASSWORD must be set in ${ENV_FILE}" >&2
    exit 1
fi

echo "Restoring ${DUMP_FILE} into database '${DB_NAME}'"
(
    cd "${PROJECT_DIR}"
    docker compose exec -T "${PG_CONTAINER}" \
        env PGPASSWORD="${DB_PASSWORD}" \
        pg_restore \
            --username "${DB_USER}" \
            --dbname "${DB_NAME}" \
            --format=custom \
            --clean \
            --if-exists \
            --no-owner \
            --no-privileges
) < "${DUMP_FILE}"

echo "Restore complete."

