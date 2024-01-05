#!/usr/bin/env python3
import time
from decimal import Decimal
from db.sqlitedb import get_sqlite_db_paths, get_sqlite_db
import lib
from lib.pair import Pair
from lib.external import CoinGeckoAPI
from util.defaults import default_error, set_params, default_result
from util.enums import NetId
from util.exceptions import DataStructureError
from util.files import Files
from util.logger import timed, logger
import util.templates as template
from util.transform import (
    sum_json_key,
    sum_json_key_10f,
    sort_dict_list,
    clean_decimal_dict_list,
    format_10f,
    merge_orderbooks,
    order_pair_by_market_cap,
)


class Generics:
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid", "exclude_unpriced", "db"]
            set_params(self, self.kwargs, self.options)
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
            if order_pair_by_market_cap(pair_str) != pair_str:
                orderbook_data = template.orderbook(pair_str, True)
            else:
                orderbook_data = template.orderbook(pair_str)
            logger.loop(orderbook_data)
            if self.netid == "ALL":
                for x in NetId:
                    if x.value != "ALL":
                        pair_obj = Pair(pair_str=pair_str, netid=self.netid, db=self.db)
                        inverse = pair_obj.inverse_requested
                        logger.info(
                            f"{pair_str} -> {pair_obj.as_str} (inverse {inverse})"
                        )
                        data = merge_orderbooks(orderbook_data, pair_obj.orderbook_data)
            else:
                pair_obj = Pair(pair_str=pair_str, netid=self.netid, db=self.db)
                inverse = pair_obj.inverse_requested
                logger.info(
                    f"{pair_str} -> {pair_obj.as_str} (inverse {inverse})"
                )
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
    def traded_pairs(
        self, days: int = 1, include_all_kmd=True, exclude_unpriced=True
    ) -> list:
        try:
            db = get_sqlite_db(db_path=self.db_path, db=self.db)
            # Returns recently traded pairs in XXX_YYY-BEP20 format
            # segwit is not yet coalesced
            pairs = db.query.get_pairs(days=days, exclude_unpriced=exclude_unpriced)
            # logger.info(pairs)
            if "error" in pairs:  # pragma: no cover
                raise DataStructureError(
                    f"'get_pairs' returned an error: {pairs['error']}"
                )
            else:
                if include_all_kmd:
                    pairs += lib.KMD_PAIRS
                    pairs = list(set(pairs))
                data = [template.pair_info(i) for i in pairs]
                data = sorted(data, key=lambda d: d["ticker_id"])
                msg = f"{len(data)} priced pairs ({days} days) from netid"
                msg += f" [{self.netid}]"
                msg += f" [exclude_unpriced {self.exclude_unpriced}]"
                return default_result(data, msg)
        except Exception as e:  # pragma: no cover
            msg = f"traded_pairs failed for netid {self.netid}!"
            db.close()
            return default_error(e, msg)

    @timed
    def traded_tickers(self, trades_days: int = 1, pairs_days: int = 7, db=None):
        try:
            if db is None:  # pragma: no cover
                db = get_sqlite_db(db_path=self.db_path)
            pairs = db.query.get_pairs(pairs_days)
            data = [
                Pair(pair_str=i, db=self.db).ticker_info(trades_days) for i in pairs
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
