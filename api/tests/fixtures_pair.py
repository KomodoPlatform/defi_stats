#!/usr/bin/env python3
import pytest
from lib.pair import Pair

from lib.cache import load_gecko_source, load_coins_config, load_generic_last_traded

coins_config = load_coins_config()
gecko_source = load_gecko_source()
generic_last_traded = load_generic_last_traded()


@pytest.fixture
def setup_kmd_ltc_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "KMD_LTC",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_ltc_kmd_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "LTC_KMD",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_morty_kmd_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "MORTY_KMD",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_dgb_doge_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "DGB_DOGE",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_doge_dgb_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "DOGE_DGB",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_kmd_dgb_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "KMD_DGB",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_btc_kmd_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "BTC_KMD",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_kmd_btc_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "KMD_BTC",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_dgb_kmd_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "DGB_KMD",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_1inch_usdc_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "1INCH-ERC20_USDC-PLG20",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_not_a_real_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "NotARealPair",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_three_ticker_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "NOT_GONNA_WORK",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )


@pytest.fixture
def setup_not_existing_pair(setup_swaps_db_data):
    db = setup_swaps_db_data
    yield Pair(
        "XYZ_123",
        testing=True,
        db=db,
        coins_config=coins_config,
        gecko_source=gecko_source,
        generic_last_traded=generic_last_traded,
    )
