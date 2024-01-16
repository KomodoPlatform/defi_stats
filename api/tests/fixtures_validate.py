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
    invert_pair,
)


@pytest.fixture
def setup_invert_pair_kmd_ltc():
    yield invert_pair("KMD_LTC")


@pytest.fixture
def setup_validate_ticker_id():
    yield validate_ticker_id("KMD_LTC")
