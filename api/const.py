#!/usr/bin/env python3
import os
import sys
from util.logger import logger
from dotenv import load_dotenv
from util.templates import Templates

load_dotenv()

# [NODE_TYPE]
# Options:
# - dev: runs both api endpoints and database sourcing / processing
# - serve: runs only the api endpoints.
# - process: runs only the database sourcing / processing
NODE_TYPE = os.getenv("NODE_TYPE") or "dev"

# Project path URLs
API_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# FastAPI configuration
API_HOST = os.getenv("API_HOST") or "0.0.0.0"
API_PORT = int(os.getenv("API_PORT")) or 7068
API_USER = os.getenv("API_USER")  # For HTTPBasicAuth
API_PASS = os.getenv("API_PASS")  # For HTTPBasicAuth

# API Keys for 3rd Party Services
FIXER_API_KEY = os.getenv("FIXER_API_KEY") or ""
if not FIXER_API_KEY:
    logger.warning(
        "FIXER_API_KEY is not set in .env file. Without this, '/api/v3/rates/fixer_io' will fail."
    )

# Path of active MM2.db database for NetId 7777 running
# in this repos docker container [if NODE_TYPE is not 'process']
LOCAL_MM2_DB_PATH_7777 = os.getenv("LOCAL_MM2_DB_PATH_7777")
if LOCAL_MM2_DB_PATH_7777 is None and NODE_TYPE != "process":
    logger.error("You need to set 'LOCAL_MM2_DB_PATH_7777' in api/.env")
    sys.exit()

# Path of active MM2.db database for NetId 8762 running
# in this repos docker container [if NODE_TYPE is not 'process']
LOCAL_MM2_DB_PATH_8762 = os.getenv("LOCAL_MM2_DB_PATH_8762")
if LOCAL_MM2_DB_PATH_8762 is None and NODE_TYPE != "process":
    logger.error("You need to set 'LOCAL_MM2_DB_PATH_8762' in api/.env")
    sys.exit()

# Paths for backups of mm2 instances running in local docker container
DB_SOURCE_PATH = f"{PROJECT_ROOT_PATH}/api/db/source"
DB_CLEAN_PATH = f"{PROJECT_ROOT_PATH}/api/db/cleaned"
DB_MASTER_PATH = f"{PROJECT_ROOT_PATH}/api/db/master"
LOCAL_MM2_DB_BACKUP_7777 = f"{DB_SOURCE_PATH}/local_MM2_7777.db"
LOCAL_MM2_DB_BACKUP_8762 = f"{DB_SOURCE_PATH}/local_MM2_8762.db"

# Paths for "master" databases, which import seed node databases
MM2_DB_PATH_ALL = f"{DB_MASTER_PATH}/MM2_all.db"
MM2_DB_PATH_7777 = f"{DB_MASTER_PATH}/MM2_7777.db"
MM2_DB_PATH_8762 = f"{DB_MASTER_PATH}/MM2_8762.db"

# Database paths as a dict, for convenience
MM2_DB_PATHS = {
    "7777": MM2_DB_PATH_7777,
    "8762": MM2_DB_PATH_8762,
    "ALL": MM2_DB_PATH_ALL,
    "temp_ALL": f"{DB_CLEAN_PATH}/temp_MM2_ALL.db",
    "temp_7777": f"{DB_CLEAN_PATH}/temp_MM2_7777.db",
    "temp_8762": f"{DB_CLEAN_PATH}/temp_MM2_8762.db",
    "local_7777": LOCAL_MM2_DB_PATH_7777,
    "local_8762": LOCAL_MM2_DB_PATH_8762,
    "local_7777_backup": LOCAL_MM2_DB_BACKUP_7777,
    "local_8762_backup": LOCAL_MM2_DB_BACKUP_8762,
}

# KomodoPlatform DeFi API config.
MM2_RPC_PORTS = {"7777": 7877, "8762": 7862, "ALL": "ALL"}
MM2_NETID = 8762  # The primary active NetId currently supported by KomodoPlatform

# Some coins may have swaps data, but are not currently in the coins repo.
CoinConfigNotFoundCoins = ["XEP", "MORTY", "RICK", "SMTF-v2"]

templates = Templates()
