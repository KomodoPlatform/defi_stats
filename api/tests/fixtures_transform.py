#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
from fixtures_db import setup_swaps_db_data, setup_swaps_db_data, setup_time


from util.transform import (
    ticker_to_market_ticker_summary,
    ticker_to_market_ticker,
    historical_trades_to_market_trades,
)

from fixtures_data import (
    trades_info,
    ticker_item,
)


@pytest.fixture
def setup_ticker_to_market_ticker():
    yield ticker_to_market_ticker(ticker_item)


@pytest.fixture
def setup_ticker_to_market_ticker_summary():
    yield ticker_to_market_ticker_summary(i=ticker_item)


@pytest.fixture
def setup_historical_trades_to_market_trades():
    yield historical_trades_to_market_trades(trades_info[0])
