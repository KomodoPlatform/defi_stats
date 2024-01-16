#!/usr/bin/env python3
import time
from db.sqlitedb import get_sqlite_db
from lib.generic import Generic
from lib.external import CoinGeckoAPI
from util.files import Files
from lib.pair import Pair
from util.helper import get_coin_variants
from util.logger import timed, logger
from util.defaults import default_error, set_params
import util.transform as transform
from const import MARKETS_PAIRS_DAYS
import lib


class Markets:
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["netid", "db"]
            set_params(self, self.kwargs, self.options)
            if self.db is None:
                self.db = get_sqlite_db(
                    netid=self.netid,
                    db=self.db,
                )
            self.files = Files(**kwargs)
            self.gecko = CoinGeckoAPI(**kwargs)
            self.generic = Generic(**kwargs)
            self.last_traded = lib.load_generic_last_traded()
            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                self.coins_config = lib.load_coins_config()
            self.segwit_coins = [i.coin for i in lib.COINS.with_segwit]
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Markets: {e}")

    @timed
    def pairs(self, days=MARKETS_PAIRS_DAYS):
        try:
            # Include unpriced, traded in last 30 days
            data = lib.load_generic_pairs()
            return data
        except Exception as e:  # pragma: no cover
            msg = f"markets.pairs failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def tickers(self, trades_days: int = 1, pairs_days: int = MARKETS_PAIRS_DAYS):
        try:
            data = self.generic.traded_tickers(pairs_days=pairs_days)
            return data
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default_error(e, msg)

    def trades(self, pair: str, days_in_past: int = 1, all=False):
        try:
            start_time = int(time.time() - 86400 * days_in_past)
            end_time = int(time.time())

            data = Pair(pair_str=pair).historical_trades(
                trade_type="all",
                start_time=start_time,
                end_time=end_time,
            )

            resp = []
            base, quote = pair.split("_")
            if all:
                logger.calc("Returning ALL")
                resp += data["ALL"]["buy"]
                resp += data["ALL"]["sell"]
            elif base in self.segwit_coins or quote in self.segwit_coins:
                bases = get_coin_variants(base, self.coins_config, segwit_only=True)
                quotes = get_coin_variants(quote, self.coins_config, segwit_only=True)
                for i in bases:
                    for j in quotes:
                        pair_str = f"{i}_{j}"
                        logger.info(pair_str)
                        if pair_str in data:
                            resp += data[pair_str]["buy"]
                            resp += data[pair_str]["sell"]
                        else:
                            logger.warning(
                                f"{pair_str} expected in variants, but not found!"
                            )
            else:
                logger.info(pair)
                resp += data[pair]["buy"]
                resp += data[pair]["sell"]
            return transform.sort_dict_list(resp, "timestamp", reverse=True)
            
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default_error(e, msg)
        