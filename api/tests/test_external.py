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

from fixtures import (
    logger,
)
from fixtures_db import (
    setup_actual_db,
    setup_swaps_db_data,
)


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


def test_get_gecko_source(setup_gecko):
    pass


def test_get_gecko_price(setup_gecko):
    gecko = setup_gecko
    assert gecko.get_gecko_price("XXX") == Decimal(0)
    assert gecko.get_gecko_price("KMD") > Decimal(0)


def test_get_gecko_mcap(setup_gecko):
    gecko = setup_gecko
    assert gecko.get_gecko_mcap("XXX") == Decimal(0)
    assert gecko.get_gecko_mcap("KMD") > Decimal(0)
