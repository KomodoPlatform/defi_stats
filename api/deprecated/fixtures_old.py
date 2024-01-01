#!/usr/bin/env python3
import os
import sys
import pytest
from decimal import Decimal

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.coin import Coin
from util.cron import Time
from lib.external import CoinGeckoAPI
from lib.dex_api import DexAPI
from util.files import Files
from util.urls import Urls
from lib.calc import Calc
from util.logger import logger
from lib.cache import Cache
from lib.cache_item import CacheItem
from lib.pair import Pair
from db.sqlitedb import SqliteDB
from lib.orderbook import Orderbook
import util.helper as helper
from db.sqlitedb import get_sqlite_db


logger.info("Loading test fixtures...")





@pytest.fixture
def setup_helper():
    yield helper
