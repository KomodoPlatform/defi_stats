#!/bin/bash

# Starting DeFi API
export PATH="$HOME/.local/bin:$PATH"
poetry env use /usr/bin/python3.10
poetry run ./sqlitedb_merge.py
