#!/bin/bash

# Starting Komodo DeFi (mm2)
echo "Starting Komodo DeFi (mm2)"
docker compose stop
cp $(pwd)/api/cache/coins $(pwd)/mm2/coins
cp $(pwd)/api/cache/coins $(pwd)/mm2_8762/coins
docker compose up -d 
docker compose logs -f --tail 23
