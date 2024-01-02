import pytest
from decimal import Decimal
from fixtures_db import setup_swaps_db_data, setup_time
from fixtures_data import swap_item
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
    is_pair_priced,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.generics import Generics
from util.logger import logger


def test_traded_pairs(setup_swaps_db_data):
    generics = Generics(db=setup_swaps_db_data, testing=True)
    r = generics.traded_pairs(include_all_kmd=False)
    assert len(r) == 4
    assert isinstance(r, list)
    assert isinstance(r[0], dict)

    r2 = generics.traded_pairs(include_all_kmd=True)
    assert len(r2) > len(r)
