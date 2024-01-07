#!/bin/bash

echo "Setup Python 3.10..."
sudo apt update && sudo apt upgrade -y
sudo apt install software-properties-common python3-apt jq -y
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10 python3.10-distutils -y
sudo ln -sf /usr/bin/python3.10 /usr/bin/python3
curl -sS https://bootstrap.pypa.io/get-pip.py | python3

wget https://raw.githubusercontent.com/KomodoPlatform/coins/master/coins
cp coins $(pwd)/mm2/coins
cp coins $(pwd)/mm2_8762/coins


rpc_password="$(openssl rand -hex 20)-E"
passphrase=$(openssl rand -hex 128)
contents=$(jq '.rpc_password = "'${rpc_password}'"' $(pwd)/mm2_8762/MM2.template.json) && echo -E "${contents}" > $(pwd)/mm2_8762/MM2.json
contents=$(jq '.passphrase = "'${passphrase}'"' $(pwd)/mm2_8762/MM2.json) && echo -E "${contents}" > $(pwd)/mm2_8762/MM2.json

rpc_password="$(openssl rand -hex 20)-E"
passphrase=$(openssl rand -hex 128)
contents=$(jq '.rpc_password = "'${rpc_password}'"' $(pwd)/mm2/MM2.template.json) && echo -E "${contents}" > $(pwd)/mm2/MM2.json
contents=$(jq '.passphrase = "'$passphrase'"' $(pwd)/mm2/MM2.json) && echo -E "${contents}" > $(pwd)/mm2/MM2.json

echo "Setting up .env [netid 7777] file..."
USER_ID=$(id -u)
GROUP_ID=$(id -g)
userpass=$(cat mm2/MM2.json | jq -r '.rpc_password')

echo "MM_CONF_PATH=/home/komodian/mm2/MM2.json" > mm2/.env
echo "MM_COINS_PATH=/home/komodian/mm2/coins" >> mm2/.env
echo "MM_LOG=/home/komodian/mm2/mm2.log" >> mm2/.env
echo "USERPASS=${userpass}" >> mm2/.env
echo "USER_ID=${USER_ID}" >> mm2/.env
echo "GROUP_ID=${GROUP_ID}" >> mm2/.env

echo "Setting up .env [netid 8762] file..."
USER_ID=$(id -u)
GROUP_ID=$(id -g)
userpass=$(cat mm2_8762/MM2.json | jq -r '.rpc_password')

echo "MM_CONF_PATH=/home/komodian/mm2/MM2.json" > mm2_8762/.env
echo "MM_COINS_PATH=/home/komodian/mm2/coins" >> mm2_8762/.env
echo "MM_LOG=/home/komodian/mm2/mm2.log" >> mm2_8762/.env
echo "USERPASS=${userpass}" >> mm2_8762/.env
echo "USER_ID=${USER_ID}" >> mm2_8762/.env
echo "GROUP_ID=${GROUP_ID}" >> mm2_8762/.env

echo "username=${USER}" > username

echo "Installing poetry..."
pip install poetry
poetry config virtualenvs.in-project true

cd $(pwd)/api
poetry update
cd ..

echo "Setup Docker..."
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh ./get-docker.sh
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
sudo systemctl enable docker.service
sudo systemctl enable containerd.service
sudo apt-get update
sudo apt-get install docker-compose-plugin
