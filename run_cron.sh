#!/bin/bash

stats_folder=/home/atomic/defi_stats/api
cd $stats_folder
/usr/bin/docker compose stop
cat $stats_folder/cache/coins/coins.json | jq .data > $stats_folder/../mm2/coins
cat $stats_folder/cache/coins/coins.json | jq .data > $stats_folder/../mm2_8762/coins
/usr/bin/docker compose build
/usr/bin/docker compose up -d

