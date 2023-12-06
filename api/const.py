#!/usr/bin/env python3
import os
from logger import logger
from dotenv import load_dotenv

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()

API_HOST = os.getenv("API_HOST") or "0.0.0.0"
API_PORT = int(os.getenv("API_PORT")) or 7068

git_raw = "https://raw.githubusercontent.com"
coins_repo_url = f"{git_raw}/KomodoPlatform/coins/master/coins"
COINS_URL = os.getenv("COINS_URL") or coins_repo_url
coins_config_repo_url = f"{git_raw}/KomodoPlatform/coins/master/utils/coins_config.json"
COINS_CONFIG_URL = os.getenv("COINS_CONFIG_URL") or coins_config_repo_url

MM2_DB_PATH_ALL = (
    os.getenv("MM2_DB_PATH_ALL") or f"{PROJECT_ROOT_PATH}/DB/MM2_all.db"
)
MM2_DB_PATH_7777 = (
    os.getenv("MM2_DB_PATH_7777") or f"{PROJECT_ROOT_PATH}/DB/MM2_7777.db"
)
MM2_DB_PATH_8762 = (
    os.getenv("MM2_DB_PATH_8762") or f"{PROJECT_ROOT_PATH}/DB/MM2_8762.db"
)
LOCAL_MM2_DB_PATH_7777 = f"{PROJECT_ROOT_PATH}/mm2/DB/8a460e332dc74d803eed3757f77bc3bdbbfa2374/MM2.db"
LOCAL_MM2_DB_PATH_8762 = f"{PROJECT_ROOT_PATH}/mm2_8762/DB/7b235c40d413d28b1f7a292f4b8660bc296db743/MM2.db"

LOCAL_MM2_DB_BACKUP_7777 = f"{PROJECT_ROOT_PATH}/DB/local_MM2_7777.db"
LOCAL_MM2_DB_BACKUP_8762 = f"{PROJECT_ROOT_PATH}/DB/local_MM2_8762.db"


MM2_HOST = "http://127.0.0.1"
MM2_RPC_PORTS = {"7777": 7877, "8762": 7862}
MM2_DB_PATHS = {
    "7777": MM2_DB_PATH_7777,
    "8762": MM2_DB_PATH_8762,
    "all": MM2_DB_PATH_ALL,
}
MM2_NETID = 7777

FIXER_API_KEY = os.getenv("FIXER_API_KEY") or ""
if not FIXER_API_KEY:
    logger.warning(
        "FIXER_API_KEY is not set in .env file. Without this, '/api/v3/rates/fixer_io' will fail."
    )
IGNORE_TICKERS = ["XEP"]
