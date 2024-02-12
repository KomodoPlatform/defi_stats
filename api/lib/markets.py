#!/usr/bin/env python3
from decimal import Decimal
from const import MARKETS_PAIRS_DAYS
from lib.pair import Pair
from lib.coins import get_segwit_coins
from util.logger import timed, logger
from util.transform import sortdata, derive, deplatform, invert, merge, clean
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

    @timed
    def tickers(self, coin=None):
        try:
            book = memcache.get_pair_orderbook_extended()
            resp = []
            data = {}
            for depair in book["orderbooks"]:
                for variant in book["orderbooks"][depair]:
                    if variant != "ALL":
                        v = variant.replace("-segwit", "")
                        if v not in data:
                            data.update({
                                v: {
                                    "last_price": Decimal(book["orderbooks"][depair][variant]["newest_price"]),
                                    "quote_volume": Decimal(book["orderbooks"][depair][variant]["quote_liquidity_coins"]),
                                    "base_volume": Decimal(book["orderbooks"][depair][variant]["base_liquidity_coins"]),
                                    "isFrozen": "0",
                                }
                            })
                        else:
                            data[v]["quote_volume"] += Decimal(book["orderbooks"][depair][variant]["quote_liquidity_coins"])
                            data[v]["base_volume"] += Decimal(book["orderbooks"][depair][variant]["base_liquidity_coins"])
                            if book["orderbooks"][depair][variant]["newest_price"] > data[v]["last_price"]:
                                data[v]["last_price"] = Decimal(book["orderbooks"][depair][variant]["newest_price"])
            for v in data:
                if data[v]['base_volume'] != 0 and data[v]['quote_volume'] != 0:
                    base, quote = derive.base_quote(pair_str=v)
                    if coin is None or coin in [base, quote]:
                        data[v] = clean.decimal_dicts(data=data[v], to_string=True)
                        resp.append({v: data[v]})
            return resp
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)

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
                    resp = merge.trades(resp, data[v])
            return sortdata.dict_lists(resp, "timestamp", reverse=True)

        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default.error(e, msg)
