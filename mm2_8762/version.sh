source userpass
curl --url "http://127.0.0.1:7862" --data "{\"method\":\"version\",\"userpass\":\"$userpass\"}"
echo ""
