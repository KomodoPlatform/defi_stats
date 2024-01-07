#!/bin/bash

# Starting Komodo DeFi (mm2)
echo "Starting Komodo DeFi (mm2)"
docker compose stop
cat $(pwd)/api/cache/coins/coins.json | jq .data > $(pwd)/mm2/coins
cat $(pwd)/api/cache/coins/coins.json | jq .data > $(pwd)/mm2_8762/coins
docker compose build
docker compose up -d 
docker compose logs -f --tail 23
