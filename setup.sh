#!/bin/bash

echo "Setting up .env file..."
USER_ID=$(id -u)
GROUP_ID=$(id -g)
userpass=$(cat mm2/MM2.json | jq -r '.rpc_password')

echo "MM2_CONF_PATH=/home/komodian/mm2/MM2.json" > mm2/.env
echo "MM_COINS_PATH=/home/komodian/mm2/coins" >> mm2/.env
echo "MM_LOG=/home/komodian/mm2/mm2.log" >> mm2/.env
echo "USERPASS=${userpass}" >> mm2/.env
echo "USER_ID=${USER_ID}" >> mm2/.env
echo "GROUP_ID=${GROUP_ID}" >> mm2/.env


ln -s $(pwd)/api/cache/coins $(pwd)/mm2/coins
