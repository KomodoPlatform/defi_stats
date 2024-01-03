#!/usr/bin/env python3
from fixtures_class import setup_dexapi
from lib.dex_api import DexAPI


def test_orderbook(setup_dexapi):
    '''
    Test orderbook() method of DexAPI class.
    TODO: Test for more response keys, see if
    any other values can be reliably static
    '''
    api = setup_dexapi
    r = api.orderbook_rpc("KMD", "LTC")
    assert "bids" in r
    assert "asks" in r

    api = DexAPI(testing=False, netid="ALL")
    r = api.orderbook_rpc("KMD", "DASH")
    assert "bids" in r
    assert "asks" in r

    r = api.orderbook_rpc("XXX", "YYY")
    assert len(r["asks"]) == 0
