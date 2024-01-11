import os
import sys
import pytest

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.cache import Cache

from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
)
from util.logger import logger
from tests.fixtures_db import setup_swaps_db_data, setup_time


def test_cache(setup_swaps_db_data):
    db = setup_swaps_db_data
    cache = Cache(testing=True, db=db)

    for i in [
        "generic_last_traded",
        "generic_pairs",
        "generic_tickers",
    ]:
        cache_item = cache.get_item(i)
        data = cache_item.save()
        logger.info(data)
        assert "error" not in data
