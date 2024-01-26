#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)


import util.transform as transform


@pytest.fixture
def setup_invert_pair_kmd_ltc():
    yield transform.invert_pair("KMD_LTC")
