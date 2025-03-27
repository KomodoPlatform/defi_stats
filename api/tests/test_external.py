#!/usr/bin/env python3
import sys
from util.cron import cron
import sqlite3
import pytest

from tests.fixtures_external import (
    setup_gecko,
    setup_gecko_info,
)
from tests.fixtures_db import (
    setup_actual_db,
    setup_swaps_db_data,
)
from util.logger import logger


def test_get_coin_ids(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_coin_ids()
    assert len(r) > 0
    assert "komodo" in r


def test_get_template(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_template()
    assert len(r["KMD"]) > 0


def test_get_gecko_coins(setup_gecko, setup_gecko_info):
    gecko = setup_gecko
    coins_info = setup_gecko_info
    r = gecko.get_gecko_coins(coins_info)
    assert len(r["komodo"]) == 2
