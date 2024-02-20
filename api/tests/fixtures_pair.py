#!/usr/bin/env python3
import pytest
from lib.pair import Pair


@pytest.fixture
def setup_kmd_ltc_pair():
    yield Pair("KMD_LTC")


@pytest.fixture
def setup_ltc_kmd_pair():
    yield Pair("LTC_KMD")


@pytest.fixture
def setup_morty_kmd_pair():
    yield Pair("MORTY_KMD")


@pytest.fixture
def setup_dgb_doge_pair():
    yield Pair("DGB_DOGE")


@pytest.fixture
def setup_doge_dgb_pair():
    yield Pair("DOGE_DGB")


@pytest.fixture
def setup_kmd_dgb_pair():
    yield Pair("KMD_DGB")


@pytest.fixture
def setup_btc_kmd_pair():
    yield Pair("BTC_KMD")


@pytest.fixture
def setup_kmd_btc_pair():
    yield Pair("KMD_BTC")


@pytest.fixture
def setup_dgb_kmd_pair():
    yield Pair("DGB_KMD")


@pytest.fixture
def setup_1inch_usdc_pair():
    yield Pair("1INCH-ERC20_USDC-PLG20")


@pytest.fixture
def setup_not_a_real_pair():
    yield Pair("NotARealPair")


@pytest.fixture
def setup_three_ticker_pair():
    yield Pair("NOT_GONNA_WORK")


@pytest.fixture
def setup_not_existing_pair():
    yield Pair("XYZ_123")
