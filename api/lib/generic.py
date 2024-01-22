#!/usr/bin/env python3
import time
from decimal import Decimal
from db.sqlitedb import get_sqlite_db
import lib
from lib.pair import Pair
from util.defaults import default_error, set_params, default_result
from util.exceptions import DataStructureError
from util.helper import get_pairs_info, get_gecko_price
from util.logger import timed, logger
from util.transform import (
    sum_json_key,
    sum_json_key_10f,
    sort_dict_list,
    clean_decimal_dict_list,
    format_10f,
    merge_orderbooks,
)
import util.templates as template
from const import GENERIC_PAIRS_DAYS


class Generic:  # pragma: no cover
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["netid", "db"]
            set_params(self, self.kwargs, self.options)

            if "gecko_source" in kwargs:
                self.gecko_source = kwargs["gecko_source"]
            else:
                self.gecko_source = lib.load_gecko_source()

            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                self.coins_config = lib.load_coins_config()

            if "last_traded_cache" in kwargs:
                self.last_traded_cache = kwargs["last_traded_cache"]
            else:
                self.last_traded_cache = lib.load_generic_last_traded()

            if self.db is None:
                self.db = get_sqlite_db(
                    netid=self.netid,
                    db=self.db,
                    coins_config=self.coins_config,
                    gecko_source=self.gecko_source,
                    last_traded_cache=self.last_traded_cache,
                )
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Generic: {e}")

    @timed
    def orderbook(self, pair_str: str = "KMD_LTC", depth: int = 100, v2: bool = False):
        try:
            if len(pair_str.split("_")) != 2:
                return {"error": "Market pair should be in `KMD_BTC` format"}
            else:
                orderbook_data = template.orderbook(pair_str)
                pair_obj = Pair(
                    pair_str=pair_str,
                    netid=self.netid,
                    db=self.db,
                    gecko_source=self.gecko_source,
                    coins_config=self.coins_config,
                    last_traded_cache=self.last_traded_cache,
                )
                vol_price_data = pair_obj.get_volumes_and_prices(days=1, v2=v2)
                data = merge_orderbooks(
                    orderbook_data, pair_obj.orderbook_data(vol_price_data)
                )
            # Standardise values
            for i in ["bids", "asks"]:
                for j in data[i]:
                    for k in ["price", "volume"]:
                        j[k] = format_10f(Decimal(j[k]))
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
            data[i] = format_10f(Decimal(data[i]))
        return data

    @timed
    def traded_pairs_info(self, days: int = GENERIC_PAIRS_DAYS) -> dict:
        """Returns basic pair info and tags as priced/unpriced"""
        try:
            # TODO: is segwit is coalesced yet?
            pairs = self.db.query.get_pairs(days=days)

            if "error" in pairs:  # pragma: no cover
                raise DataStructureError(
                    f"'get_pairs' returned an error: {pairs['error']}"
                )
            else:
                pairs_dict = {"priced_gecko": [], "unpriced": []}
                for pair_str in pairs:
                    pair_split = pair_str.split("_")
                    base_price = get_gecko_price(pair_split[0], self.gecko_source)
                    quote_price = get_gecko_price(pair_split[1], self.gecko_source)
                    if base_price > 0 and quote_price > 0:
                        pairs_dict["priced_gecko"].append(pair_str)
                    else:  # pragma: no cover
                        pairs_dict["unpriced"].append(pair_str)

                for pair_str in lib.KMD_PAIRS:
                    if pair_str not in pairs:
                        pair_split = pair_str.split("_")
                        base_price = get_gecko_price(pair_split[0], self.gecko_source)
                        quote_price = get_gecko_price(pair_split[1], self.gecko_source)
                        if base_price > 0 and quote_price > 0:
                            pairs_dict["priced_gecko"].append(pair_str)
                        else:  # pragma: no cover
                            pairs_dict["unpriced"].append(pair_str)

                priced_pairs = get_pairs_info(pairs_dict["priced_gecko"], True)
                unpriced_pairs = get_pairs_info(pairs_dict["unpriced"], False)
                resp = sort_dict_list(priced_pairs + unpriced_pairs, "ticker_id")

                for i in resp:
                    if i["ticker_id"] in self.last_traded_cache:
                        i["last_trade"] = self.last_traded_cache[i["ticker_id"]][
                            "last_swap"
                        ]
                return resp
        except Exception as e:  # pragma: no cover
            msg = f"traded_pairs failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def traded_tickers_old(self, trades_days: int = 1, pairs_days: int = 7):
        try:
            pairs = self.db.query.get_pairs(days=pairs_days)
            data = [
                Pair(
                    pair_str=i,
                    db=self.db,
                    gecko_source=self.gecko_source,
                    coins_config=self.coins_config,
                    last_traded_cache=self.last_traded_cache,
                ).ticker_info(trades_days)
                for i in pairs
            ]
            data = [i for i in data if i is not None]
            data = clean_decimal_dict_list(data, to_string=True, rounding=10)
            data = sort_dict_list(data, "ticker_id")
            data = {
                "last_update": int(time.time()),
                "pairs_count": len(data),
                "swaps_count": int(sum_json_key(data, "trades_24hr")),
                "combined_volume_usd": sum_json_key_10f(data, "volume_usd_24hr"),
                "combined_liquidity_usd": sum_json_key_10f(data, "liquidity_in_usd"),
                "data": data,
            }
            msg = f"traded_tickers_old for netid {self.netid} complete!"
            return default_result(data, msg)
        except Exception as e:  # pragma: no cover
            msg = f"traded_tickers_old failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def traded_tickers(self, trades_days: int = 1, pairs_days: int = 7):
        try:
            pairs = self.db.query.get_pairs(days=pairs_days)
            data = [
                Pair(
                    pair_str=i,
                    db=self.db,
                    gecko_source=self.gecko_source,
                    coins_config=self.coins_config,
                    last_traded_cache=self.last_traded_cache,
                ).ticker_info(trades_days, v2=True)
                for i in pairs
            ]
            data = [i for i in data if i is not None]
            data = clean_decimal_dict_list(data, to_string=True, rounding=10)
            data = sort_dict_list(data, "ticker_id")
            data = {
                "last_update": int(time.time()),
                "pairs_count": len(data),
                "swaps_count": int(sum_json_key(data, "trades_24hr")),
                "combined_volume_usd": sum_json_key_10f(data, "volume_usd_24hr"),
                "combined_liquidity_usd": sum_json_key_10f(data, "liquidity_in_usd"),
                "data": data,
            }
            msg = f"traded_tickers for netid {self.netid} complete!"
            return default_result(data, msg)
        except Exception as e:  # pragma: no cover
            msg = f"traded_tickers failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def last_traded(self):
        try:
            data = self.db.query.get_pairs_last_traded()
            return data
        except Exception as e:  # pragma: no cover
            msg = f"pairs_last_traded failed for netid {self.netid}!"
            return default_error(e, msg)
