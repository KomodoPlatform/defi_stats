#!/usr/bin/env python3
import os
import sys
import pytest


API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.external import CoinGeckoAPI
from lib.dex_api import DexAPI
from util.files import Files
from util.urls import Urls
from lib.markets import Markets
from lib.cache import Cache
from lib.stats_api import StatsAPI
import util.helper as helper
import lib


@pytest.fixture
def setup_dexapi():
    yield DexAPI(testing=True)


@pytest.fixture
def setup_statsapi(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield StatsAPI(testing=True, db=db)


@pytest.fixture
def setup_markets():
    yield Markets(testing=True)


@pytest.fixture
def setup_cache_item():
    yield lib.CacheItem(testing=True)


@pytest.fixture
def setup_pairs_cache():
    yield lib.CacheItem("generic_pairs", testing=True).data


@pytest.fixture
def setup_last_traded_cache():
    yield lib.CacheItem("generic_last_traded", testing=True).data


@pytest.fixture
def setup_files():
    yield Files(testing=True)


@pytest.fixture
def setup_urls():
    yield Urls(testing=True)


@pytest.fixture
def setup_cache():
    yield Cache(testing=True)


@pytest.fixture
def setup_gecko():
    yield CoinGeckoAPI(testing=True)


@pytest.fixture
def setup_gecko_coin_ids(setup_gecko):
    gecko = setup_gecko
    yield gecko.get_gecko_coin_ids()


@pytest.fixture
def setup_gecko_info(setup_gecko):
    gecko = setup_gecko
    yield gecko.get_gecko_info()


@pytest.fixture
def setup_helper():
    yield helper
