#!/bin/bash
source .env
echo "run uvicorn main:app --host ${API_HOST} --port ${API_PORT} --reload"
poetry run uvicorn main:app --host ${API_HOST} --port ${API_PORT} --reload
