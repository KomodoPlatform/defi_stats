#!/usr/bin/env python3
from fixtures import (
    setup_dexapi, setup_dgb_kmd_str_pair, setup_fake_db,
    setup_kmd_dgb_tuple_pair, logger, setup_swaps_db_data,
    setup_time
)
from lib.dex_api import DexAPI


def test_orderbook(setup_dexapi, setup_dgb_kmd_str_pair, setup_kmd_dgb_tuple_pair):
    '''
    Test orderbook() method of DexAPI class.
    TODO: Test for more response keys, see if
    any other values can be reliably static
    '''
    api = setup_dexapi
    pair = setup_dgb_kmd_str_pair
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r

    pair = setup_kmd_dgb_tuple_pair
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r

    api = DexAPI(testing=False)
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r

    r = api.orderbook(("XXX", "YYY"))
    assert "error" in r
