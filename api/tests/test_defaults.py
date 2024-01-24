#!/usr/bin/env python3
import util.cron as cron
from tests.fixtures_class import setup_dexapi
from util.defaults import arg_defaults, set_params
from const import DEXAPI_8762_HOST


def test_arg_defaults():
    """
    Test orderbook() method of DexAPI class.
    TODO: Test for more response keys, see if
    any other values can be reliably static
    """

    class Test:
        def __init__(self, **kwargs) -> None:
            self.options = []
            for i in arg_defaults().values():
                self.options += i
            set_params(self, kwargs, self.options)

    test = Test(order_by_mcap=False, source_url="https://app.komodoplatform.com/")
    assert not test.reverse
    assert test.wal
    assert not test.order_by_mcap
    assert test.db_path is None
    assert test.source_url == "https://app.komodoplatform.com/"
    assert test.end > int(cron.now_utc()) - 10
    assert test.mm2_host == DEXAPI_8762_HOST
    assert test.trigger == 0
    assert test.coin == "KMD"
    assert test.loglevel == "debug"
    assert test.msg == ""
