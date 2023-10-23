#!/bin/bash

# Starting Komodo DeFi (mm2)
echo "Starting Komodo DeFi (mm2)"
docker compose stop
cp api/cache/coins mm2/coins
docker compose up -d 

# Starting DeFi API
echo "Starting DeFi API"
cd api
source .env
export PATH="$HOME/.local/bin:$PATH"
poetry run uvicorn main:app --host ${API_HOST} --port ${API_PORT} --reload
