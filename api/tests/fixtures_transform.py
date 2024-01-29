#!/usr/bin/env python3
import os
import sys
import pytest

from tests.fixtures_data import trades_info, get_ticker_item

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

import util.transform as transform


@pytest.fixture
def setup_ticker_to_market_ticker():
    yield transform.ticker_to_market_ticker(get_ticker_item())



@pytest.fixture
def setup_ticker_to_statsapi_24h():
    yield transform.ticker_to_statsapi_summary(get_ticker_item())


@pytest.fixture
def setup_ticker_to_statsapi_7d():
    yield transform.ticker_to_statsapi_summary(get_ticker_item("7d"))


@pytest.fixture
def setup_ticker_to_market_ticker_summary():
    yield transform.ticker_to_market_ticker_summary(i=get_ticker_item())


@pytest.fixture
def setup_historical_trades_to_market_trades():
    yield transform.historical_trades_to_market_trades(trades_info[0])
