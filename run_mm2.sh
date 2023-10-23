#!/bin/bash

# Starting Komodo DeFi (mm2)
echo "Starting Komodo DeFi (mm2)"
docker compose stop
cp /home/admin/defi_stats/api/cache/coins /home/admin/defi_stats/mm2/coins
docker compose up -d 
