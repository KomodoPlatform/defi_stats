#!/usr/bin/env python3
from typing import Dict
from const import API_ROOT_PATH
import db
import lib
from util.files import Files
from lib.coins import get_segwit_coins
from util.logger import logger, timed
from util.transform import sortdata
import util.defaults as default
import util.memcache as memcache
import util.templates as template
import util.transform as transform


class Orderbook:
    def __init__(self, pair_obj, coins_config: Dict | None = None, **kwargs):
        try:
            self.kwargs = kwargs
            self.pair = pair_obj
            self.base = self.pair.base
            self.quote = self.pair.quote
            self.options = ["mm2_host"]
            default.params(self, self.kwargs, self.options)
            self.coins_config = coins_config
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()

            self.pg_query = db.SqlQuery()
            segwit_coins = [i for i in get_segwit_coins()]
            self.base_is_segwit_coin = self.base in segwit_coins
            self.quote_is_segwit_coin = self.quote in segwit_coins

        except Exception as e:  # pragma: no cover
            logger.error({"error": f"{type(e)} Failed to init Orderbook: {e}"})


@timed
def get_and_parse(base: str, quote: str, coins_config: Dict):
    try:
        pair = f"{base}_{quote}"
        data = template.orderbook(pair_str=pair)

        if base not in coins_config or quote not in coins_config:
            pass
        elif coins_config[base]["wallet_only"]:
            pass
        elif coins_config[quote]["wallet_only"]:
            pass

        elif memcache.get("testing") is not None:
            files = Files()
            path = f"{API_ROOT_PATH}/tests/fixtures/orderbook"
            fn = f"{pair}.json"
            fixture = files.load_jsonfile(f"{path}/{fn}")
            if fixture is None:
                fn = f"{transform.invert_pair(pair)}.json"
                fixture = files.load_jsonfile(f"{path}/{fn}")
            if fixture is not None:
                data = fixture
            is_reversed = pair != sortdata.order_pair_by_market_cap(pair)
            data["pair"] = pair
            if is_reversed:
                data = transform.invert_orderbook(data)
            data = transform.label_bids_asks(data, pair)

        else:
            cached = memcache.get(f"orderbook_{pair}")
            if cached is None:
                t = lib.OrderbookRpcThread(base, quote)
                t.start()
            else:
                data = cached

        msg = f"orderbook.get_and_parse {pair} complete!"
    except Exception as e:  # pragma: no cover
        msg = f"orderbook.get_and_parse {pair} failed: {e}! Returning template!"
        logger.error(msg)
    return default.result(data=data, msg=msg, loglevel="muted")
