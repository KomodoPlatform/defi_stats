#!/bin/bash

# Starting DeFi API
cd /home/admin/defi_stats/api
source .env
export PATH="$HOME/.local/bin:$PATH"
poetry env use /usr/bin/python3.10
poetry run uvicorn main:app --host ${API_HOST} --port ${API_PORT} --reload
