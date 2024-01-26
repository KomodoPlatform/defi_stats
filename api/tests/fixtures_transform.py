#!/usr/bin/env python3
import os
import sys
import pytest

from tests.fixtures_data import trades_info, get_ticker_item

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from util.transform import (
    pairs_to_gecko,
    ticker_to_statsapi_summary,
    ticker_to_gecko,
    ticker_to_market_ticker_summary,
    ticker_to_market_ticker,
    historical_trades_to_market_trades,
)


@pytest.fixture
def setup_ticker_to_market_ticker():
    yield ticker_to_market_ticker(get_ticker_item())


@pytest.fixture
def setup_pairs_to_gecko(setup_pairs_cache):
    yield pairs_to_gecko(setup_pairs_cache)


@pytest.fixture
def setup_ticker_to_gecko():
    yield ticker_to_gecko(get_ticker_item())


@pytest.fixture
def setup_ticker_to_statsapi_24h():
    yield ticker_to_statsapi_summary(get_ticker_item(), "24hr")


@pytest.fixture
def setup_ticker_to_statsapi_7d():
    yield ticker_to_statsapi_summary(get_ticker_item("7d"), "7d")


@pytest.fixture
def setup_ticker_to_market_ticker_summary():
    yield ticker_to_market_ticker_summary(i=get_ticker_item())


@pytest.fixture
def setup_historical_trades_to_market_trades():
    yield historical_trades_to_market_trades(trades_info[0])
