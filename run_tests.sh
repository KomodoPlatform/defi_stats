#!/bin/bash

# Starting DeFi API
cd $(pwd)/api
clear
poetry run pytest -vv 
