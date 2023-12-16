#!/bin/bash

# Starting Komodo DeFi (mm2)
echo "Starting Komodo DeFi (mm2)"
docker compose stop
cp $(pwd)/api/cache/coins/coins.json $(pwd)/mm2/coins
cp $(pwd)/api/cache/coins/coins.json $(pwd)/mm2_8762/coins
docker compose build --no-cache
docker compose up -d 
docker compose logs -f --tail 23
