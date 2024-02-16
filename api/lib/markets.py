#!/usr/bin/env python3
from decimal import Decimal
from lib.pair import Pair
from lib.coins import get_segwit_coins
from util.logger import timed, logger
from util.transform import sortdata, derive, invert, merge, clean, template
from util.cron import cron
import util.defaults as default
import util.memcache as memcache


class Markets:
    def __init__(self) -> None:
        try:
            self.netid = 8762
            self.segwit_coins = [i for i in get_segwit_coins()]
            self.coins_config = memcache.get_coins_config()
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Markets: {e}")


    # TODO: Cache this
    def trades(self, pair_str: str, days_in_past: int = 1, all_variants: bool = False):
        try:
            pairs_last_trade_cache = memcache.get_pair_last_traded()
            start_time = int(cron.now_utc() - 86400 * days_in_past)
            end_time = int(cron.now_utc())
            data = Pair(
                pair_str=pair_str, pairs_last_trade_cache=pairs_last_trade_cache
            ).historical_trades(
                start_time=start_time,
                end_time=end_time,
            )
            resp = []
            base, quote = derive.base_quote(pair_str)
            if all_variants:
                resp = merge.trades(resp, data["ALL"])
            else:
                variants = derive.pair_variants(
                    pair_str=pair_str, segwit_only=True, coins_config=self.coins_config
                )
                for v in variants:
                    if v in data:
                        resp = merge.trades(resp, data[v])
                    elif invert.pair(v) in data:
                        resp = merge.trades(resp, data[invert.pair(v)])
                    else:
                        logger.warning(f"Variant {v} not found in data!")
            return sortdata.dict_lists(resp, "timestamp", reverse=True)

        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)
