#!/usr/bin/env python3
import os
from dotenv import load_dotenv

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))


load_dotenv()
git_raw = "https://raw.githubusercontent.com"
coins_repo_url = f'{git_raw}/KomodoPlatform/coins/master/coins'
coins__config_repo_url = f'{git_raw}/KomodoPlatform/coins/master/utils/coins_config.json'
API_HOST = os.getenv('API_HOST') or "0.0.0.0"
API_PORT = int(os.getenv('API_PORT')) or 7068
COINS_URL = os.getenv('COINS_URL') or coins_repo_url
COINS_CONFIG_URL = os.getenv('COINS_CONFIG_URL') or coins__config_repo_url
MM2_DB_PATH = f"{API_ROOT_PATH}/cache/MM2.db"
SCRIPT_PATH = os.path.realpath(os.path.dirname(__file__))
