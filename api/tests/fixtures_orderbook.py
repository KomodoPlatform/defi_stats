#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
from util.files import Files
from util.logger import logger
from lib.orderbook import Orderbook
from fixtures_db import (
    setup_kmd_dgb_pair,
    setup_kmd_ltc_pair,
    setup_kmd_btc_pair,
    setup_ltc_kmd_pair,
    setup_dgb_kmd_pair,
    setup_btc_kmd_pair,
)

logger.info("Loading test fixtures...")

files = Files(testing=True)


@pytest.fixture
def setup_kmd_dgb_orderbook(setup_kmd_dgb_pair):
    pair = setup_kmd_dgb_pair
    yield Orderbook(pair=pair, testing=True)


@pytest.fixture
def setup_dgb_kmd_orderbook(setup_dgb_kmd_pair):
    pair = setup_dgb_kmd_pair
    yield Orderbook(pair=pair, testing=True)


@pytest.fixture
def setup_kmd_ltc_orderbook(setup_kmd_ltc_pair):
    pair = setup_kmd_ltc_pair
    yield Orderbook(pair=pair, testing=True)


@pytest.fixture
def setup_ltc_kmd_orderbook(setup_ltc_kmd_pair):
    pair = setup_ltc_kmd_pair
    yield Orderbook(pair=pair, testing=True)


@pytest.fixture
def setup_kmd_btc_orderbook(setup_kmd_btc_pair):
    pair = setup_kmd_btc_pair
    yield Orderbook(pair=pair, testing=True)


@pytest.fixture
def setup_kmd_btc_orderbook(setup_btc_kmd_pair):
    pair = setup_btc_kmd_pair
    yield Orderbook(pair=pair, testing=True)
