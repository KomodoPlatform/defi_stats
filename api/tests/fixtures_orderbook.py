#!/usr/bin/env python3
import os
import pytest
from util.logger import logger
from lib.orderbook import Orderbook

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger.info("Loading test fixtures...")


@pytest.fixture
def setup_kmd_dgb_orderbook(setup_kmd_dgb_pair):
    pair = setup_kmd_dgb_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_dgb_kmd_orderbook(setup_dgb_kmd_pair):
    pair = setup_dgb_kmd_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_dgb_doge_orderbook(setup_dgb_doge_pair):
    pair = setup_dgb_doge_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_doge_dgb_orderbook(setup_doge_dgb_pair):
    pair = setup_doge_dgb_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_kmd_ltc_orderbook(setup_kmd_ltc_pair):
    pair = setup_kmd_ltc_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_ltc_kmd_orderbook(setup_ltc_kmd_pair):
    pair = setup_ltc_kmd_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_kmd_btc_orderbook(setup_kmd_btc_pair):
    pair = setup_kmd_btc_pair
    yield Orderbook(pair_obj=pair)


@pytest.fixture
def setup_btc_kmd_orderbook(setup_btc_kmd_pair):
    yield Orderbook(pair_obj=setup_btc_kmd_pair)


@pytest.fixture
def setup_kmd_btc_segwit_orderbook_data(setup_files):
    files = setup_files
    file = f"{API_ROOT_PATH}/tests/fixtures/orderbook/KMD_BTC-segwit.json"
    orderbook = files.load_jsonfile(file)
    yield orderbook


@pytest.fixture
def setup_kmd_btc_bep20_orderbook_data(setup_files):
    files = setup_files
    file = f"{API_ROOT_PATH}/tests/fixtures/orderbook/KMD_BTC-BEP20.json"
    orderbook = files.load_jsonfile(file)
    yield orderbook


@pytest.fixture
def setup_kmd_btc_orderbook_data(setup_files):
    files = setup_files
    file = f"{API_ROOT_PATH}/tests/fixtures/orderbook/KMD_BTC.json"
    orderbook = files.load_jsonfile(file)
    yield orderbook
