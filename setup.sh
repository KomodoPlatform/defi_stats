#!/bin/bash

echo "Setup Python 3.10..."
sudo apt update && sudo apt upgrade -y
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.10

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

echo "Setting up .env file..."
USER_ID=$(id -u)
GROUP_ID=$(id -g)
userpass=$(cat mm2_8762/MM2.json | jq -r '.rpc_password')

echo "MM2_CONF_PATH=/home/komodian/mm2/MM2.json" > mm2_8762/.env
echo "MM_COINS_PATH=/home/komodian/mm2/coins" >> mm2_8762/.env
echo "MM_LOG=/home/komodian/mm2/mm2.log" >> mm2_8762/.env
echo "USERPASS=${userpass}" >> mm2_8762/.env
echo "USER_ID=${USER_ID}" >> mm2_8762/.env
echo "GROUP_ID=${GROUP_ID}" >> mm2_8762/.env

echo "Installing poetry..."
pip install poetry

cd $(pwd)/api
poetry update
cd ..
