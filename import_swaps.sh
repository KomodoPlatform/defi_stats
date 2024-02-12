#!/bin/bash

# Starting DeFi API
cd $(pwd)/api/scripts
clear
poetry run ./import_swaps.py
