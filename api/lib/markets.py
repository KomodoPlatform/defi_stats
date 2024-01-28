#!/usr/bin/env python3
from const import MARKETS_PAIRS_DAYS
from lib.generic import Generic
from lib.pair import Pair
from lib.coins import get_segwit_coins
from util.logger import timed, logger
from util.transform import sortdata
import util.cron as cron
import util.defaults as default
import util.helper as helper
import util.memcache as memcache


class Markets:
    def __init__(self, **kwargs) -> None:
        try:
            self.netid = 8762
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.generic = Generic(**kwargs)
            self.segwit_coins = [i for i in get_segwit_coins()]
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Markets: {e}")

    @timed
    def tickers(self, trades_days: int = 1, pairs_days: int = MARKETS_PAIRS_DAYS):
        try:
            # Todo: Set this cache up for temporal filtering
            tickers = memcache.get_tickers()
            return tickers
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)

    # TODO: Cache this
    def trades(self, pair: str, days_in_past: int = 1, all=False):
        try:
            last_traded_cache = memcache.get_last_traded()
            start_time = int(cron.now_utc() - 86400 * days_in_past)
            end_time = int(cron.now_utc())
            data = Pair(
                pair_str=pair, last_traded_cache=last_traded_cache
            ).historical_trades(
                start_time=start_time,
                end_time=end_time,
            )

            resp = []
            base, quote = helper.base_quote_from_pair(pair)
            if all:
                resp += data["ALL"]["buy"]
                resp += data["ALL"]["sell"]
            elif base in self.segwit_coins or quote in self.segwit_coins:
                bases = helper.get_coin_variants(
                    base,
                    segwit_only=True,
                )
                quotes = helper.get_coin_variants(
                    quote,
                    segwit_only=True,
                )
                for i in bases:
                    for j in quotes:
                        pair_str = f"{i}_{j}"
                        if pair_str in data:
                            resp += data[pair_str]["buy"]
                            resp += data[pair_str]["sell"]
                        else:
                            logger.warning(
                                f"{pair_str} expected in variants, but not found!"
                            )
            else:
                resp += data[pair]["buy"]
                resp += data[pair]["sell"]
            return sortdata.sort_dict_list(resp, "timestamp", reverse=True)

        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)
