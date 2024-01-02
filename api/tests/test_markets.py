import pytest
from decimal import Decimal
from fixtures_data import swap_item
from util.helper import (
    get_mm2_rpc_port,
    get_netid_filename,
    get_chunks,
    get_price_at_finish,
    is_pair_priced,
)
from const import MM2_DB_PATH_7777, MM2_DB_PATH_8762, MM2_DB_PATH_ALL
from lib.markets import Markets

markets = Markets(netid="8762")
