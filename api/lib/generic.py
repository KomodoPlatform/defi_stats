#!/usr/bin/env python3
from decimal import Decimal
import db
import lib
from lib.coins import get_pairs_info, get_gecko_price, get_kmd_pairs

from util.exceptions import DataStructureError
from util.logger import timed, logger
import util.cron as cron
import util.defaults as default
import util.memcache as memcache
import util.templates as template
import util.transform as transform


class Generic:  # pragma: no cover
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.pg_query = db.SqlQuery()

        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Generic: {e}")

    @timed
    def pairs(self, days: int = 90) -> dict:
        """Returns basic pair info and tags as priced/unpriced"""
        try:
            pairs = self.pg_query.get_pairs(days=days)
            # We dont merge these down until later, where required.
            # pairs = sorted(list(set([transform.strip_pair_platforms(i) for i in data])))
            if "error" in pairs:  # pragma: no cover
                raise DataStructureError(
                    f"'get_pairs' returned an error: {pairs['error']}"
                )
            else:
                resp = get_pairs_status(pairs)
                resp = transform.clean_decimal_dict_list(resp)
                last_traded_cache = memcache.get_last_traded()
                for i in resp:
                    first_last_swap = template.first_last_swap()
                    if last_traded_cache is not None:
                        if i["ticker_id"] in last_traded_cache:
                            x = last_traded_cache[i["ticker_id"]]
                            first_last_swap = transform.clean_decimal_dict(x)
                    i.update(first_last_swap)
                msg = f"{len(pairs)} pairs traded in the last {days} days"
                return default.result(
                    data=resp, msg=msg, loglevel="loop"
                )
        except Exception as e:  # pragma: no cover
            msg = "pairs failed!"
            return default.error(e, msg)

    @timed
    def orderbook(
        self,
        pair_str: str = "KMD_LTC",
        depth: int = 100,
        all: bool = False,
    ):
        try:
            if len(pair_str.split("_")) != 2:
                return {"error": "Market pair should be in `KMD_BTC` format"}
            else:
                pair_obj = lib.Pair(pair_str=pair_str)
                data = pair_obj.orderbook.for_pair(depth=depth, all=all)
            # Standardise values
            for i in ["bids", "asks"]:
                for j in data[i]:
                    for k in ["price", "volume"]:
                        j[k] = transform.format_10f(Decimal(j[k]))
            data["bids"] = data["bids"][: int(depth)][::-1]
            data["asks"] = data["asks"][::-1][: int(depth)]
        except Exception as e:  # pragma: no cover
            err = {"error": f"Generic.orderbook: {e}"}
            logger.warning(err)
            return template.orderbook(pair_str)
        for i in [
            "total_asks_base_vol",
            "total_bids_base_vol",
            "total_asks_quote_vol",
            "total_bids_quote_vol",
            "total_asks_base_usd",
            "total_bids_quote_usd",
            "liquidity_usd",
            "volume_usd_24hr",
        ]:
            data[i] = transform.format_10f(Decimal(data[i]))
        return data

    @timed
    def traded_tickers(self, trades_days: int = 1, pairs_days: int = 7):
        try:
            last_traded_cache = memcache.get_last_traded()
            if last_traded_cache is None:
                msg = "skipping traded_tickers, last_traded_cache is None"
                return default.result(msg=msg, loglevel='warning', data=None)

            coins_config = memcache.get_coins_config()
            if coins_config is None:
                msg = "skipping traded_tickers, coins_config is None"
                return default.result(msg=msg, loglevel='warning', data=None)

            suffix = transform.get_suffix(trades_days)
            ts = cron.now_utc() - pairs_days * 86400

            logger.info(f"{len(last_traded_cache.keys())} pairs in last_traded_cache")

            # Filter out those older than requested time and
            pairs = sorted([
                i for i in last_traded_cache
                if last_traded_cache[i]["last_swap_time"] > ts
            ])
            logger.info(f"{len(pairs)} pairs in last 90 days")
            data = [
                lib.Pair(
                    pair_str=i,
                    last_traded_cache=last_traded_cache,
                    coins_config=coins_config,
                ).ticker_info(trades_days, all=False)
                for i in pairs
            ]
            data = [i for i in data if i is not None]
            data = transform.clean_decimal_dict_list(data, to_string=True, rounding=10)
            data = transform.sort_dict_list(data, "ticker_id")
            data = {
                "last_update": int(cron.now_utc()),
                "pairs_count": len(data),
                "swaps_count": int(transform.sum_json_key(data, f"trades_{suffix}")),
                "combined_volume_usd": transform.sum_json_key_10f(
                    data, f"combined_volume_usd"
                ),
                "combined_liquidity_usd": transform.sum_json_key_10f(
                    data, "liquidity_in_usd"
                ),
                "data": data,
            }
            msg = f"Traded_tickers complete! {len(pairs)} pairs traded"
            msg += f" in last {pairs_days} days"
            return default.result(data, msg, loglevel="calc")
        except Exception as e:  # pragma: no cover
            msg = "traded_tickers failed!"
            return default.error(e, msg)

    @timed
    def last_traded(self):
        try:
            data = self.pg_query.pair_last_trade()
            for i in data:
                transform.clean_decimal_dict(data[i])
            return data
        except Exception as e:  # pragma: no cover
            msg = "pairs_last_traded failed!"
            return default.error(e, msg)


@timed
def get_pairs_status(pairs):
    pairs_dict = {"priced_gecko": [], "unpriced": []}
    # Process pairs returned from DB
    pairs = list(set(pairs + get_kmd_pairs()))
    for pair_str in pairs:
        pair_split = pair_str.split("_")
        base_price = get_gecko_price(pair_split[0])
        quote_price = get_gecko_price(pair_split[1])
        if base_price > 0 and quote_price > 0:
            pairs_dict["priced_gecko"].append(pair_str)
        else:  # pragma: no cover
            pairs_dict["unpriced"].append(pair_str)
    priced_pairs = get_pairs_info(pairs_dict["priced_gecko"], True)
    unpriced_pairs = get_pairs_info(pairs_dict["unpriced"], False)
    return transform.sort_dict_list(priced_pairs + unpriced_pairs, "ticker_id")
