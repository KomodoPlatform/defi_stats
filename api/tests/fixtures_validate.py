#!/usr/bin/env python3
import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)


from util.transform import invert


@pytest.fixture
def setup_invert_pair_kmd_ltc():
    yield invert.pair("KMD_LTC")
