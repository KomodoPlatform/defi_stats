#!/usr/bin/env python3
import os
import pytest
from util.logger import logger

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger.info("Loading test fixtures...")


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
