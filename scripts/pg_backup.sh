#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/pg_backup.sh [options]

Options:
  -e PATH   Path to the .env file (default: <repo>/api/.env)
  -o PATH   Directory to write backups into (default: <repo>/backups/postgres)
  -c NAME   Docker Compose service/container name (default: pgsqldb)
  -p PATH   Compose project directory (default: repo root)
  -h        Show this help message

The script dumps the Postgres database defined in api/.env by exec'ing
`pg_dump` inside the running pgsqldb container. Ensure `docker compose up`
has already started the database service.
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/api/.env}"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/backups/postgres}"
PG_CONTAINER="${PG_CONTAINER:-pgsqldb}"
PROJECT_DIR="${PROJECT_DIR:-${REPO_ROOT}}"

while getopts "e:o:c:p:h" opt; do
    case "${opt}" in
        e) ENV_FILE="$(realpath "${OPTARG}")" ;;
        o) OUTPUT_DIR="$(realpath "${OPTARG}")" ;;
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

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Env file not found: ${ENV_FILE}" >&2
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

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

timestamp="$(date -u +"%Y%m%dT%H%M%SZ")"
backup_path="${OUTPUT_DIR}/${DB_NAME}_${timestamp}.dump"

echo "Creating backup at ${backup_path}"
(
    cd "${PROJECT_DIR}"
    docker compose exec -T "${PG_CONTAINER}" \
        env PGPASSWORD="${DB_PASSWORD}" \
        pg_dump \
            --username "${DB_USER}" \
            --dbname "${DB_NAME}" \
            --format=custom \
            --no-owner \
            --no-privileges
) > "${backup_path}"

echo "Backup complete."
echo "File: ${backup_path}"

