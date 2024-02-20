#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.coins import Coin
import util.memcache as memcache

coins_config = memcache.get_coins_config()


@pytest.fixture
def setup_coin_ltc():
    yield Coin(coin="LTC", coins_config=coins_config)


@pytest.fixture
def setup_coin_bad():
    yield Coin(coin="CFADFC", coins_config=coins_config)


@pytest.fixture
def setup_coin_btc():
    yield Coin(coin="BTC", coins_config=coins_config)


@pytest.fixture
def setup_coin_btc_bep20():
    yield Coin(coin="BTC-BEP20", coins_config=coins_config)


@pytest.fixture
def setup_coin_kmd():
    yield Coin(coin="KMD", coins_config=coins_config)


@pytest.fixture
def setup_coin_ltc_segwit():
    yield Coin(coin="LTC-segwit", coins_config=coins_config)


@pytest.fixture
def setup_coin_doc():
    yield Coin(coin="DOC", coins_config=coins_config)


@pytest.fixture
def setup_coin_atom():
    yield Coin(coin="ATOM", coins_config=coins_config)
