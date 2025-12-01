#!/usr/bin/env bash
set -euo pipefail

SUPERUSER="${POSTGRES_USER:-postgres}"
APP_DB="${POSTGRES_DATABASE:-${POSTGRES_DB:-stats_swaps}}"
APP_USER="${POSTGRES_USERNAME:-${POSTGRES_USER:-gleecian}}"

# Allow overriding the application password separately via POSTGRES_APP_PASSWORD.
APP_PASSWORD="${POSTGRES_APP_PASSWORD:-${POSTGRES_PASSWORD:-}}"

if [[ -z "${APP_PASSWORD}" ]]; then
    echo "[initdb] Set POSTGRES_PASSWORD or POSTGRES_APP_PASSWORD for application user provisioning." >&2
    exit 1
fi

psql_super() {
    psql -v ON_ERROR_STOP=1 --username "${SUPERUSER}" "$@"
}

quote_ident_sql() {
    local value=${1//\"/\"\"}
    printf '"%s"' "${value}"
}

quote_literal_sql() {
    local value=${1//\'/\'\'}
    printf "'%s'" "${value}"
}

ROLE_IDENT=$(quote_ident_sql "${APP_USER}")
ROLE_LITERAL=$(quote_literal_sql "${APP_USER}")
PASS_LITERAL=$(quote_literal_sql "${APP_PASSWORD}")
DB_IDENT=$(quote_ident_sql "${APP_DB}")
DB_LITERAL=$(quote_literal_sql "${APP_DB}")

echo "[initdb] Ensuring role '${APP_USER}' exists..."
ROLE_EXISTS=$(psql_super --dbname="postgres" -tAc "SELECT 1 FROM pg_roles WHERE rolname=${ROLE_LITERAL};" || true)
if [[ -z "${ROLE_EXISTS}" ]]; then
    psql_super --dbname="postgres" -c "CREATE ROLE ${ROLE_IDENT} LOGIN PASSWORD ${PASS_LITERAL};"
else
    psql_super --dbname="postgres" -c "ALTER ROLE ${ROLE_IDENT} WITH LOGIN PASSWORD ${PASS_LITERAL};"
fi

echo "[initdb] Ensuring database '${APP_DB}' exists..."
DB_EXISTS=$(psql_super --dbname="postgres" -tAc "SELECT 1 FROM pg_database WHERE datname=${DB_LITERAL};" || true)
if [[ -z "${DB_EXISTS}" ]]; then
    psql_super --dbname="postgres" -c "CREATE DATABASE ${DB_IDENT} OWNER ${ROLE_IDENT};"
else
    psql_super --dbname="postgres" -c "ALTER DATABASE ${DB_IDENT} OWNER TO ${ROLE_IDENT};"
fi

echo "[initdb] Applying sane defaults..."
psql_super --dbname="${APP_DB}" -c "ALTER ROLE ${ROLE_IDENT} SET search_path TO public;"
psql_super --dbname="postgres" -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_IDENT} TO ${ROLE_IDENT};"

if [[ "${INIT_CREATE_DEFISWAPS:-1}" == "1" ]]; then
    echo "[initdb] Creating table 'defi_swaps' (if missing)..."
    psql_super --dbname="${APP_DB}" <<'EOSQL'
CREATE TABLE IF NOT EXISTS defi_swaps (
    id SERIAL PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    pair TEXT,
    pair_std TEXT,
    pair_reverse TEXT,
    pair_std_reverse TEXT,
    trade_type TEXT,
    is_success SMALLINT DEFAULT 0,
    taker_amount NUMERIC,
    taker_coin TEXT,
    taker_coin_ticker TEXT,
    taker_coin_platform TEXT,
    taker_gui TEXT,
    taker_pubkey TEXT,
    taker_version TEXT,
    taker_coin_usd_price NUMERIC,
    maker_amount NUMERIC,
    maker_coin TEXT,
    maker_coin_ticker TEXT,
    maker_coin_platform TEXT,
    maker_gui TEXT,
    maker_pubkey TEXT,
    maker_version TEXT,
    maker_coin_usd_price NUMERIC,
    price NUMERIC,
    reverse_price NUMERIC,
    started_at BIGINT,
    finished_at BIGINT,
    duration BIGINT,
    validated BOOLEAN DEFAULT FALSE,
    last_updated BIGINT DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS defi_swaps_uuid_idx ON defi_swaps (uuid);
CREATE INDEX IF NOT EXISTS defi_swaps_finished_at_idx ON defi_swaps (finished_at);
EOSQL
fi

echo "[initdb] Database bootstrap complete."
