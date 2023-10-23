#!/bin/bash

# Starting DeFi API
cd api
source .env
export PATH="$HOME/.local/bin:$PATH"
poetry env use /usr/bin/python3.10
poetry run uvicorn /home/admin/defi_stats/main:app --host ${API_HOST} --port ${API_PORT} --reload
