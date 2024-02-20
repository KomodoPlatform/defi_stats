#!/bin/bash
# Starting DeFi API
cd $(pwd)/api/scripts
clear
echo $(poetry run ./import_swaps.py $1 $2 $3)
