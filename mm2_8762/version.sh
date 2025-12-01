userpass=$(cat MM2.json | jq -r '.rpc_password')
curl --url "http://127.0.0.1:7862" --data "{\"method\":\"version\",\"userpass\":\"$userpass\"}"
echo ""
