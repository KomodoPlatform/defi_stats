#!/bin/bash

cd $(pwd)/api
docker compose stop
cat $(pwd)/cache/coins/coins.json | jq .data > $(pwd)/../mm2/coins
cat $(pwd)/cache/coins/coins.json | jq .data > $(pwd)/../mm2_8762/coins
docker compose build
docker compose up -d 

echo "To follow logs:"
echo " - docker compose logs -f --tail 23"
echo " - docker compose logs fastapi -f --tail 23"
echo " - docker compose logs pgsqldb -f --tail 23"
echo " - docker compose logs komodefi -f --tail 23"
echo " - docker compose logs komodefi_8762 -f --tail 23"
