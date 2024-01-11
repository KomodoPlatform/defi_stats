#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from util.validate import (
    validate_ticker_id,
)

from util.transform import (
    reverse_ticker,
)


@pytest.fixture
def setup_reverse_ticker_kmd_ltc():
    yield reverse_ticker("KMD_LTC")


@pytest.fixture
def setup_validate_ticker_id():
    yield validate_ticker_id("KMD_LTC")
