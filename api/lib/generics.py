#!/usr/bin/env python3
import time
from decimal import Decimal
from db.sqlitedb import get_sqlite_db_paths, get_sqlite_db
import lib
from lib.cache_load import get_gecko_price_and_mcap, load_gecko_source, load_coins_config
from lib.external import CoinGeckoAPI
from lib.pair import Pair
from util.defaults import default_error, set_params, default_result
from util.enums import NetId
from util.exceptions import DataStructureError
from util.files import Files
from util.helper import get_pairs_info
from util.logger import timed, logger
from util.transform import (
    sum_json_key,
    sum_json_key_10f,
    sort_dict_list,
    clean_decimal_dict_list,
    format_10f,
    merge_orderbooks,
    order_pair_by_market_cap,
)
import util.templates as template
from const import GENERIC_PAIRS_DAYS


class Generics:
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid", "db"]
            set_params(self, self.kwargs, self.options)
            if "gecko_source" in kwargs:
                self.gecko_source = kwargs["gecko_source"]
            else:
                # logger.loop("Getting gecko source for Generics")
                self.gecko_source = load_gecko_source(testing=self.testing)

            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                # logger.loop("Getting coins_config for Generics")
                self.coins_config = load_coins_config(testing=self.testing)
            self.db_path = get_sqlite_db_paths(netid=self.netid)
            self.files = Files(netid=self.netid, testing=self.testing, db=self.db)
            self.gecko = CoinGeckoAPI(testing=self.testing)
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Generics: {e}")

    @timed
    def get_orderbook(self, pair_str: str = "KMD_LTC", depth: int = 100):
        try:
            logger.info(f"Getting orderbook for {pair_str} on {self.netid}")
            if len(pair_str.split("_")) != 2:
                return {"error": "Market pair should be in `KMD_BTC` format"}
            if (
                order_pair_by_market_cap(pair_str, gecko_source=self.gecko_source)
                != pair_str
            ):
                orderbook_data = template.orderbook(pair_str, True)
            else:
                orderbook_data = template.orderbook(pair_str)
            logger.loop(orderbook_data)
            if self.netid == "ALL":
                for x in NetId:
                    if x.value != "ALL":
                        pair_obj = Pair(
                            pair_str=pair_str,
                            netid=self.netid,
                            db=self.db,
                            gecko_source=self.gecko_source,
                            coins_config=self.coins_config,
                        )
                        inverse = pair_obj.inverse_requested
                        logger.info(
                            f"{pair_str} -> {pair_obj.as_str} (inverse {inverse})"
                        )
                        data = merge_orderbooks(orderbook_data, pair_obj.orderbook_data)
            else:
                pair_obj = Pair(
                    pair_str=pair_str,
                    netid=self.netid,
                    db=self.db,
                    gecko_source=self.gecko_source,
                    coins_config=self.coins_config,
                )
                inverse = pair_obj.inverse_requested
                logger.info(f"{pair_str} -> {pair_obj.as_str} (inverse {inverse})")
                data = merge_orderbooks(orderbook_data, pair_obj.orderbook_data)
            # Standardise values
            for i in ["bids", "asks"]:
                for j in data[i]:
                    for k in ["price", "volume"]:
                        j[k] = format_10f(Decimal(j[k]))
            data["bids"] = data["bids"][: int(depth)][::-1]
            data["asks"] = data["asks"][::-1][: int(depth)]
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
        except Exception as e:  # pragma: no cover
            err = {"error": f"{e}"}
            logger.warning(err)
            return template.orderbook(pair_str)

    @timed
    def traded_pairs_info(self, days: int = GENERIC_PAIRS_DAYS) -> dict:
        """Returns basic pair info and tags as priced/unpriced"""
        try:
            # TODO: is segwit is coalesced yet?
            db = get_sqlite_db(db_path=self.db_path, db=self.db)
            pairs = db.query.get_pairs(days=days)

            # logger.info(pairs)
            if "error" in pairs:  # pragma: no cover
                raise DataStructureError(
                    f"'get_pairs' returned an error: {pairs['error']}"
                )
            else:
                pairs_dict = {"priced_gecko": [], "unpriced": []}
                for pair_str in pairs:
                    # logger.info(pair_str)
                    pair_split = pair_str.split("_")
                    base_price = get_gecko_price_and_mcap(
                        pair_split[0], self.gecko_source
                    )[0]
                    quote_price = get_gecko_price_and_mcap(
                        pair_split[1], self.gecko_source
                    )[0]
                    if base_price > 0 and quote_price > 0:
                        pairs_dict["priced_gecko"].append(pair_str)
                    else:
                        pairs_dict["unpriced"].append(pair_str)

                for pair_str in lib.KMD_PAIRS:
                    # logger.info(pair_str)
                    pair_split = pair_str.split("_")
                    base_price = get_gecko_price_and_mcap(
                        pair_split[0], self.gecko_source
                    )[0]
                    quote_price = get_gecko_price_and_mcap(
                        pair_split[1], self.gecko_source
                    )[0]
                    if base_price > 0 and quote_price > 0:
                        pairs_dict["priced_gecko"].append(pair_str)
                    else:
                        pairs_dict["unpriced"].append(pair_str)

                priced_pairs = get_pairs_info(pairs_dict["priced_gecko"], True)
                unpriced_pairs = get_pairs_info(pairs_dict["unpriced"], False)
                resp = sort_dict_list(priced_pairs + unpriced_pairs, "ticker_id")
                return resp
        except Exception as e:  # pragma: no cover
            msg = f"traded_pairs failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def traded_tickers(self, trades_days: int = 1, pairs_days: int = 7, db=None):
        try:
            if db is None:  # pragma: no cover
                db = get_sqlite_db(db_path=self.db_path)
            pairs = db.query.get_pairs(pairs_days)
            data = [
                Pair(
                    pair_str=i,
                    db=self.db,
                    gecko_source=self.gecko_source,
                    coins_config=self.coins_config,
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
            db.close()
            msg = f"traded_tickers for netid {self.netid} complete!"
            return default_result(data, msg)
        except Exception as e:  # pragma: no cover
            msg = f"traded_tickers failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def last_traded(self):
        try:
            db = get_sqlite_db(db_path=self.db_path)
            data = db.query.get_pairs_last_traded()
            return data
        except Exception as e:  # pragma: no cover
            msg = f"pairs_last_traded failed for netid {self.netid}!"
            return default_error(e, msg)
