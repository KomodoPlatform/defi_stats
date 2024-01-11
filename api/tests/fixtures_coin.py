#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.coin import Coin


@pytest.fixture
def setup_coin_ltc():
    yield Coin(coin="LTC")


@pytest.fixture
def setup_coin_bad():
    yield Coin(coin="CFADFC")


@pytest.fixture
def setup_coin_btc():
    yield Coin(coin="BTC")


@pytest.fixture
def setup_coin_btc_bep20():
    yield Coin(coin="BTC-BEP20")


@pytest.fixture
def setup_coin():
    yield Coin()


@pytest.fixture
def setup_coin_ltc_segwit():
    yield Coin(coin="LTC-segwit")


@pytest.fixture
def setup_coin_doc():
    yield Coin(coin="DOC")


@pytest.fixture
def setup_coin_atom():
    yield Coin(coin="ATOM")
