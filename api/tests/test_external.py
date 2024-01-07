#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
from fixtures_class import (
    setup_gecko,
    setup_gecko_info,
    setup_gecko_coin_ids,
)
from fixtures_db import (
    setup_actual_db,
    setup_swaps_db_data,
)
from util.logger import logger


def test_get_gecko_coin_ids(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_coin_ids()
    assert len(r) > 0
    assert "komodo" in r


def test_get_gecko_info(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_info()
    assert len(r["KMD"]) > 0


def test_get_gecko_coins(setup_gecko, setup_gecko_info, setup_gecko_coin_ids):
    gecko = setup_gecko
    coin_ids = setup_gecko_coin_ids
    coins_info = setup_gecko_info
    r = gecko.get_gecko_coins(coins_info, coin_ids)
    assert len(r["komodo"]) == 2
