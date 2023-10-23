#!/usr/bin/env python3
import os
import time
import json
import sqlite3
import requests
from typing import Any
from collections import OrderedDict
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from logger import logger
import const
from helper import (
    sum_json_key, sum_json_key_10f, format_10f,
    list_json_key, sort_dict_list, order_pair_by_market_cap,
    set_pair_as_tuple
)

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
api_root_path = os.path.dirname(os.path.abspath(__file__))


class Files:
    def __init__(self, testing: bool = False):
        if testing:
            folder = f"{api_root_path}/tests/fixtures"
        else:
            folder = f"{api_root_path}/cache"
        # Coins repo data
        self.coins = f"{folder}/coins"
        self.coins_config = f"{folder}/coins_config.json"
        # For CoinGecko endpoints
        self.gecko_source = f"{folder}/gecko/source_cache.json"
        self.gecko_tickers = f"{folder}/gecko/ticker_cache.json"
        self.gecko_pairs = f"{folder}/gecko/pairs_cache.json"


class Cache:
    def __init__(self, testing: bool = False, db_path=const.MM2_DB_PATH, DB=None):
        self.db_path = db_path
        self.DB = DB
        self.testing = testing
        self.utils = Utils(self.testing)
        self.files = Files(self.testing)
        self.load = self.Load(
            files=self.files,
            utils=self.utils
        )
        self.calc = self.Calc(
            db_path=self.db_path,
            load=self.load,
            testing=self.testing,
            utils=self.utils,
            DB=self.DB
        )
        self.save = self.Save(
            calc=self.calc,
            testing=self.testing,
            files=self.files,
            utils=self.utils
        )
        # Coins repo data
        self.coins = None
        self.coins_config = None
        # For CoinGecko endpoints
        self.gecko_source = None
        self.gecko_tickers = None
        self.refresh()
        logger.info("Cache initialized...")

    def refresh(self):
        # Coins repo data
        self.coins = self.load.coins()
        self.coins_config = self.load.coins_config()
        # For CoinGecko endpoints
        self.gecko_source = self.load.gecko_source()
        self.gecko_pairs = self.load.gecko_pairs()
        self.gecko_tickers = self.load.gecko_tickers()

    class Load:
        def __init__(self, files, utils):
            self.files = files
            self.utils = utils

        # Coins repo data
        def coins(self):
            return self.utils.load_jsonfile(self.files.coins)

        def coins_config(self):
            return self.utils.load_jsonfile(self.files.coins_config)

        # For CoinGecko endpoints
        def gecko_source(self):
            return self.utils.load_jsonfile(self.files.gecko_source)

        def gecko_tickers(self):
            return self.utils.load_jsonfile(self.files.gecko_tickers)

        def gecko_pairs(self):
            return self.utils.load_jsonfile(self.files.gecko_pairs)

    class Calc:
        def __init__(self, db_path, load, testing, utils, DB=None):
            self.DB = DB
            self.db_path = db_path
            self.load = load
            self.testing = testing
            self.utils = utils
            self.gecko = CoinGeckoAPI(self.testing)

        # For CoinGecko endpoints
        def gecko_source(self):
            return self.gecko.get_gecko_source()

        def gecko_pairs(self, days: int = 7, exclude_unpriced: bool = True) -> list:
            try:
                DB = self.utils.get_db(self.db_path, self.DB)
                pairs = DB.get_pairs(days)
                logger.debug(f"{len(pairs)} pairs ({days} days)")
                data = [
                    Pair(i, self.db_path, self.testing, DB=DB).info
                    for i in pairs
                    if len(set(i).intersection(self.gecko.priced_coins)) == 2
                    or not exclude_unpriced
                ]
                data = sorted(data, key=lambda d: d['ticker_id'])
                logger.debug(f"{len(data)} priced pairs ({days} days)")
                return data
            except Exception as e:  # pragma: no cover
                logger.error(
                    f"{type(e)} Error in [Cache.calc.gecko_pairs]: {e}")
                return {
                    "error": f"{type(e)} Error in [Cache.calc.gecko_pairs]: {e}"
                }  # pragma: no cover

        def gecko_tickers(self, trades_days: int = 1, pairs_days: int = 7):
            DB = self.utils.get_db(self.db_path, self.DB)
            pairs = DB.get_pairs(pairs_days)
            logger.debug(
                f"Calculating [gecko_tickers] {len(pairs)} pairs ({pairs_days}d)")
            data = [
                Pair(i, self.db_path, self.testing,
                     DB=DB).gecko_tickers(trades_days)
                for i in pairs
            ]
            # Remove None values (from coins without price)
            data = [i for i in data if i is not None]
            data = sort_dict_list(data, "ticker_id")
            data = self.utils.clean_decimal_dict_list(
                data, to_string=True, rounding=10)
            return {
                "last_update": int(time.time()),
                "pairs_count": len(data),
                "swaps_count": int(sum_json_key(data, "trades_24hr")),
                "combined_volume_usd": sum_json_key_10f(data, "volume_usd_24hr"),
                "data": data
            }

    class Save:
        '''
        Updates cache json files.
        '''

        def __init__(self, calc, files, utils, testing=False):
            self.calc = calc
            self.files = files
            self.testing = testing
            self.utils = utils

        def save(self, path, data):
            if not isinstance(data, (dict, list)):
                raise TypeError(
                    f"Invalid data type: {type(data)}, must be dict or list")
            elif self.testing:  # pragma: no cover
                if path in [self.files.gecko_source, self.files.coins_config]:
                    logger.info(f"Validated {path} data")
                    return {"result": f"Validated {path} data"}
            with open(path, "w+") as f:
                json.dump(data, f, indent=4)
                logger.info(f"Updated {path}")
                return {
                    "result": f"Updated {path}"
                }

        # Coins repo data
        def coins_config(self, url=const.COINS_CONFIG_URL):
            data = self.utils.download_json(url)
            if data is not None:
                return self.save(self.files.coins_config, data)

        def coins(self, url=const.COINS_URL):
            data = self.utils.download_json(url)
            if data is not None:
                return self.save(self.files.coins, data)

        # For CoinGecko endpoints
        def gecko_source(self):  # pragma: no cover
            data = self.calc.gecko_source()
            if "error" in data:
                logger.warning(data["error"])
            else:
                self.save(self.files.gecko_source, data)

        def gecko_pairs(self):  # pragma: no cover
            data = self.calc.gecko_pairs()
            return self.save(self.files.gecko_pairs, data)

        def gecko_tickers(self):  # pragma: no cover
            data = self.calc.gecko_tickers()
            return self.save(self.files.gecko_tickers, data)


class CoinGeckoAPI:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.utils = Utils()
        self.templates = Templates()
        self.files = Files(self.testing)
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config)
        self.gecko_source = self.load_gecko_source()
        self.priced_coins = sorted(list(self.gecko_source.keys()))

    def load_gecko_source(self):
        return self.utils.load_jsonfile(self.files.gecko_source)

    def get_gecko_coin_ids_list(self) -> list:
        coin_ids = list(
            set(
                [
                    self.coins_config[i]["coingecko_id"]
                    for i in self.coins_config
                    if self.coins_config[i]["coingecko_id"]
                    not in ["na", "test-coin", ""]
                ]
            )
        )
        coin_ids.sort()
        return coin_ids

    def get_gecko_info_dict(self):
        coins_info = {}
        for coin in self.coins_config:
            native_coin = coin.split("-")[0]
            coin_id = self.coins_config[coin]["coingecko_id"]
            if coin_id not in ["na", "test-coin", ""]:
                coins_info.update({coin: self.templates.gecko_info(coin_id)})
                if native_coin not in coins_info:
                    coins_info.update(
                        {native_coin: self.templates.gecko_info(coin_id)}
                    )
        return coins_info

    def get_gecko_coins_dict(self, gecko_info: dict, coin_ids: list):
        gecko_coins = {}
        for coin_id in coin_ids:
            gecko_coins.update({coin_id: []})
        for coin in gecko_info:
            coin_id = gecko_info[coin]["coingecko_id"]
            gecko_coins[coin_id].append(coin)
        return gecko_coins

    def get_gecko_source(self):
        param_limit = 200
        coin_ids = self.get_gecko_coin_ids_list()
        coins_info = self.get_gecko_info_dict()
        gecko_coins = self.get_gecko_coins_dict(coins_info, coin_ids)
        coin_id_chunks = list(self.utils.get_chunks(coin_ids, param_limit))
        for chunk in coin_id_chunks:
            chunk_ids = ",".join(chunk)
            try:
                params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
                url = f"https://api.coingecko.com/api/v3/simple/price?{params}"
                r = requests.get(url)
                if r.status_code != 200:  # pragma: no cover
                    raise Exception(f"Invalid response: {r.status_code}")
                gecko_source = r.json()

            except Exception as e:  # pragma: no cover
                error = {
                    "error": f"{type(e)} Error in [get_gecko_source]: {e}"}
                logger.error(error)
                return error
            try:
                for coin_id in gecko_source:
                    try:
                        coins = gecko_coins[coin_id]
                        for coin in coins:
                            if "usd" in gecko_source[coin_id]:
                                coins_info[coin].update(
                                    {"usd_price": gecko_source[coin_id]["usd"]}
                                )
                            if "usd_market_cap" in gecko_source[coin_id]:
                                coins_info[coin].update(
                                    {
                                        "usd_market_cap": gecko_source[coin_id][
                                            "usd_market_cap"
                                        ]
                                    }
                                )
                    except Exception as e:  # pragma: no cover
                        error = (
                            f"CoinGecko ID request/response mismatch [{coin_id}] [{e}]"
                        )
                        logger.warning(error)

            except Exception as e:  # pragma: no cover
                error = {
                    "error": f"{type(e)} Error in [get_gecko_source]: {e}"}
                logger.error(error)
                return error
        return coins_info


class Time:
    def __init__(self, testing: bool = False):
        self.testing = testing

    def now(self):  # pragma: no cover
        return int(time.time())

    def hours_ago(self, num):
        return int(time.time()) - (num * 60 * 60)

    def days_ago(self, num):
        return int(time.time()) - (num * 60 * 60) * 24


class Orderbook:
    def __init__(self, pair, testing: bool = False):
        self.pair = pair
        self.testing = testing
        self.utils = Utils(testing)
        self.templates = Templates()
        self.dexapi = DexAPI(testing)
        pass

    def for_pair(self, endpoint=False, depth=100):
        try:
            orderbook_data = OrderedDict()
            orderbook_data["ticker_id"] = self.pair.as_str
            orderbook_data["timestamp"] = "{}".format(
                int(datetime.now().strftime("%s"))
            )
            data = self.get_and_parse(endpoint)
            orderbook_data["bids"] = data["bids"][:depth][::-1]
            orderbook_data["asks"] = data["asks"][::-1][:depth]
            if endpoint:
                total_bids_base_vol = sum(
                    [
                        Decimal(i[1])
                        for i in orderbook_data["bids"]
                    ]
                )
                total_asks_base_vol = sum(
                    [
                        Decimal(i[1])
                        for i in orderbook_data["asks"]
                    ]
                )
            else:
                #logger.debug(f"Total bids: {orderbook_data['bids']}")
                total_bids_base_vol = sum(
                    [
                        Decimal(i["base_max_volume"])
                        for i in orderbook_data["bids"]
                    ]
                )
                total_asks_base_vol = sum(
                    [
                        Decimal(i["base_max_volume"])
                        for i in orderbook_data["asks"]
                    ]
                )
            orderbook_data["total_asks_base_vol"] = total_asks_base_vol
            orderbook_data["total_bids_base_vol"] = total_bids_base_vol
            return orderbook_data
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error in [Orderbook.for_pair]: {e}")
            return []

    def related_orderbooks_list(self):
        '''
        Gets a list of orderbooks for all coins related to the pair
        (including wrapped tokens). Since the v2 Orderbook returns
        both segwit and non-segwit orders for UTXO coins, we need to
        exclude those tickers to avoid duplicate uuids in the output.
        '''
        related_pairs = self.utils.get_related_pairs(self.pair)
        orderbooks_list = [
            self.dexapi.orderbook(pair)
            for pair in related_pairs
        ]
        return orderbooks_list

    def merge_orders(self, orders_list, order_type):
        return [
            i[order_type][j] for i in orders_list
            if i[order_type] != []
            for j in range(len(i[order_type]))
        ]

    def merge_order_vols(self, orders_list, vol_type):
        return sum(
            [
                Decimal(i[vol_type]["decimal"])
                for i in orders_list
                if i[vol_type]
            ]
        )

    def get_and_parse(self, endpoint=False):
        orderbook = self.templates.orderbook(self.pair.base, self.pair.quote)
        orderbooks_list = self.related_orderbooks_list()
        for i in ["asks", "bids"]:
            orderbook[i] = self.merge_orders(orderbooks_list, i)


        bids_converted_list = []
        asks_converted_list = []
        for bid in orderbook["bids"]:
            if Decimal(bid["price"]["decimal"]) != Decimal(0):
                bid_price = self.utils.round_to_str(
                    bid["price"]["decimal"], 13)
                bid_vol = self.utils.round_to_str(
                    bid["base_max_volume"]["decimal"], 13)
                if endpoint:
                    bids_converted_list.append(
                        [
                            bid["price"]["decimal"],
                            bid["base_max_volume"]["decimal"]
                        ]
                    )
                else:
                    bids_converted_list.append({
                        "price": bid["price"]["decimal"],
                        "base_max_volume": bid["base_max_volume"]["decimal"]
                    })

        for ask in orderbook["asks"]:
            if Decimal(ask["price"]["decimal"]) != Decimal(0):
                if endpoint:
                    asks_converted_list.append(
                        [
                            ask["price"]["decimal"],
                            ask["base_max_volume"]["decimal"]
                        ]
                    )
                else:
                    asks_converted_list.append({
                        "price": ask["price"]["decimal"],
                        "base_max_volume": ask["base_max_volume"]["decimal"]
                    })
        orderbook["bids"] = bids_converted_list
        orderbook["asks"] = asks_converted_list
        return orderbook


class Pair:
    """
    Allows for referencing pairs as a string or tuple.
    To standardise like CEX pairs, the higher Mcap coin is always second.
    e.g. DOGE_BTC, not BTC_DOGE
    """

    def __init__(self, pair, db_path=const.MM2_DB_PATH, testing: bool = False, DB=None):
        self.DB = DB
        self.testing = testing
        self.db_path = db_path
        self.files = Files(testing=self.testing)
        self.utils = Utils()
        self.templates = Templates()
        self.orderbook = Orderbook(pair=self, testing=self.testing)
        self.gecko_source = self.utils.load_jsonfile(
            self.files.gecko_source)
        self.as_tuple = order_pair_by_market_cap(
            set_pair_as_tuple(pair), self.gecko_source)
        self.as_str = self.as_tuple[0] + "_" + self.as_tuple[1]
        self.base = self.as_tuple[0]
        self.quote = self.as_tuple[1]
        self.info = {
            "ticker_id": self.as_str,
            "pool_id": self.as_str,
            "base": self.base,
            "target": self.quote
        }

    def historical_trades(
        self,
        trade_type: const.TradeType,
        limit: int = 100,
        start_time: int = 0,
        end_time: int = 0
    ):
        """Returns trades for this pair."""
        try:
            if end_time == 0:
                end_time = int(time.time())
            trades_info = []
            DB = self.utils.get_db(self.db_path, self.DB)
            swaps_for_pair = DB.get_swaps_for_pair(
                self.as_tuple,
                limit=limit,
                trade_type=trade_type,
                start_time=start_time,
                end_time=end_time
            )
            logger.debug(
                f"{len(swaps_for_pair)} swaps_for_pair: {self.as_str}")
            for swap in swaps_for_pair:
                trade_info = OrderedDict()
                price = Decimal(swap["taker_amount"]) / \
                    Decimal(swap["maker_amount"])
                trade_info["trade_id"] = swap["uuid"]
                trade_info["base_ticker"] = self.base
                trade_info["target_ticker"] = self.quote
                trade_info["price"] = format_10f(price)
                trade_info["base_volume"] = format_10f(swap["maker_amount"])
                trade_info["target_volume"] = format_10f(swap["taker_amount"])
                trade_info["timestamp"] = swap["started_at"]
                trade_info["type"] = swap["trade_type"]
                trades_info.append(trade_info)
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [Pair.trades]: {e}")
            trades_info = []

        average_price = self.get_average_price(trades_info)
        buys = list_json_key(trades_info, "type", "buy")
        sells = list_json_key(trades_info, "type", "sell")
        buys = sort_dict_list(buys, "timestamp", reverse=True)
        sells = sort_dict_list(sells, "timestamp", reverse=True)

        data = {
            "ticker_id": self.as_str,
            "start_time": str(start_time),
            "end_time": str(end_time),
            "limit": str(limit),
            "trades_count": str(len(trades_info)),
            "sum_base_volume_buys": sum_json_key_10f(buys, "base_volume"),
            "sum_base_volume_sells": sum_json_key_10f(sells, "base_volume"),
            "sum_target_volume_buys": sum_json_key_10f(buys, "target_volume"),
            "sum_target_volume_sells": sum_json_key_10f(sells, "target_volume"),
            "average_price": format_10f(average_price),
            "buy": buys,
            "sell": sells
        }

        return data

    def get_average_price(self, trades_info):
        if len(trades_info) > 0:
            return sum_json_key(
                trades_info, "price") / len(trades_info)
        return 0

    def get_volumes_and_prices(
        self,
        days: int = 1,
    ):
        """
        Iterates over list of swaps to get data for CMC summary endpoint
        """
        suffix = self.utils.get_suffix(days)
        data = self.templates.volumes_and_prices(suffix)

        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
        try:
            swaps_for_pair = self.DB.get_swaps_for_pair(
                self.as_tuple,
                start_time=timestamp
            )
        except Exception as e:
            logger.warning(
                f"{type(e)} Error in [Pair.get_volumes_and_prices]: {e}")
            return data
        base_price = self.utils.get_gecko_usd_price(
            self.base, self.gecko_source)
        quote_price = self.utils.get_gecko_usd_price(
            self.quote, self.gecko_source)
        data["base"] = "KMD"
        data["quote"] = "LTC"
        data["base_price"] = base_price
        data["quote_price"] = quote_price
        num_swaps = len(swaps_for_pair)
        data["trades_24hr"] = num_swaps
        swap_prices = self.get_swap_prices(swaps_for_pair)
        swaps_volumes = self.get_swaps_volumes(swaps_for_pair)
        data["base_volume"] = swaps_volumes[0]
        data["quote_volume"] = swaps_volumes[1]
        data["base_volume_usd"] = Decimal(
            swaps_volumes[0]) * Decimal(base_price)
        data["quote_volume_usd"] = Decimal(
            swaps_volumes[1]) * Decimal(quote_price)
        data["combined_volume_usd"] = data["base_volume_usd"] + \
            data["quote_volume_usd"]

        if len(swap_prices) > 0:
            # TODO: using timestamps as an index works for now,
            # but breaks when two swaps have the same timestamp.
            last_swap = self.DB.get_last_price_for_pair(
                self.base, self.quote
            )
            highest_price = max(swap_prices.values())
            lowest_price = min(swap_prices.values())
            newest_price = swap_prices[max(swap_prices.keys())]
            oldest_price = swap_prices[min(swap_prices.keys())]
            price_change = Decimal(newest_price) - Decimal(oldest_price)
            pct_change = (Decimal(newest_price) - Decimal(oldest_price)) / Decimal(
                100
            )
            data[f"highest_price_{suffix}"] = highest_price
            data[f"lowest_price_{suffix}"] = lowest_price
            data["last_price"] = last_swap["price"]
            data["last_trade"] = last_swap["timestamp"]
            data[f"price_change_percent_{suffix}"] = pct_change
            data[f"price_change_{suffix}"] = price_change
        return data

    def gecko_tickers(self, days=1):
        # TODO: ps: in order for CoinGecko to show +2/-2% depth,
        # DEX has to provide the formula for +2/-2% depth.
        try:
            DB = self.utils.get_db(self.db_path, self.DB)
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            data = self.get_volumes_and_prices(days)
            orderbook = self.orderbook.for_pair(endpoint=False)
            suffix = self.utils.get_suffix(days)
            base_price = Decimal(
                self.utils.get_gecko_usd_price(
                    self.base, self.gecko_source)
            )
            quote_price = Decimal(
                self.utils.get_gecko_usd_price(
                    self.quote, self.gecko_source)
            )
            return {
                "ticker_id": self.as_str,
                "pool_id": self.as_str,
                "base_currency": self.base,
                "target_currency": self.quote,
                "last_price": format_10f(data["last_price"]),
                "last_trade": f'{data["last_trade"]}',
                "trades_24hr": f'{data["trades_24hr"]}',
                "base_volume": data["base_volume"],
                "target_volume": data["quote_volume"],
                "base_usd_price": base_price,
                "target_usd_price": quote_price,
                "bid": self.utils.find_highest_bid(orderbook),
                "ask": self.utils.find_lowest_ask(orderbook),
                "high": format_10f(data[f"highest_price_{suffix}"]),
                "low": format_10f(data[f"lowest_price_{suffix}"]),
                "volume_usd_24hr": format_10f(data["combined_volume_usd"])
            }
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [Pair.ticker]: {e}")
            return {}

    def get_swap_prices(self, swaps_for_pair):
        data = {}
        [
            data.update({i["started_at"]: Decimal(
                i["taker_amount"]) / Decimal(i["maker_amount"])})
            for i in swaps_for_pair
        ]
        return data

    def get_swaps_volumes(self, swaps_for_pair):
        try:
            return [
                sum_json_key_10f(swaps_for_pair, "maker_amount"),
                sum_json_key_10f(swaps_for_pair, "taker_amount")
            ]
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error in [get_swaps_volumes]: {e}")
            return [0, 0]


class SqliteDB:
    def __init__(self, path_to_db, dict_format=False, testing: bool = False):
        self.utils = Utils()
        self.files = Files(testing)
        self.testing = testing
        self.conn = sqlite3.connect(path_to_db)
        if dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        self.gecko_source = self.utils.load_jsonfile(self.files.gecko_source)

    def close(self):
        self.conn.close()

    def get_pairs(self, days: int = 7) -> list:
        """
        Returns an alphabetically sorted list of pairs
        (as a list of tuples) with at least one successful
        swap in the last 'x' days. ('BASE', 'REL') tuples
        are sorted by market cap to conform to CEX standards.
        """
        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
        sql = f"SELECT DISTINCT maker_coin_ticker, taker_coin_ticker FROM stats_swaps \
                WHERE started_at > {timestamp} AND is_success=1;"
        self.sql_cursor.execute(sql)
        pairs = self.sql_cursor.fetchall()
        sorted_pairs = [tuple(sorted(pair)) for pair in pairs]
        pairs = list(set(sorted_pairs))
        data = sorted([order_pair_by_market_cap(
            pair, self.gecko_source) for pair in pairs])
        return data

    def get_swaps_for_pair(
        self,
        pair: tuple,
        trade_type: const.TradeType = const.TradeType.ALL,
        limit: int = 0,
        start_time: int = 0,
        end_time: int = 0
    ) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        try:
            if end_time == 0:
                end_time = int(time.time())
            if limit == 0:
                t = (
                    start_time,
                    end_time,
                    pair[0],
                    pair[1]
                )
                sql = "SELECT * FROM stats_swaps WHERE \
                        started_at > ? \
                        AND started_at < ? \
                        AND maker_coin_ticker=? \
                        AND taker_coin_ticker=? \
                        AND is_success=1 \
                        ORDER BY started_at DESC;"
            else:
                t = (
                    start_time,
                    end_time,
                    pair[0],
                    pair[1],
                    limit
                )
                sql = "SELECT * FROM stats_swaps WHERE \
                        started_at > ? \
                        AND started_at < ? \
                        AND maker_coin_ticker=? \
                        AND taker_coin_ticker=? \
                        AND is_success=1 \
                        ORDER BY started_at DESC \
                        LIMIT ?;"

            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            self.sql_cursor.execute(
                sql,
                t,
            )
            data = self.sql_cursor.fetchall()
            swaps_for_pair_a_b = [dict(row) for row in data]

            for swap in swaps_for_pair_a_b:
                swap["trade_type"] = "buy"
            if limit == 0:
                t = (
                    start_time,
                    end_time,
                    pair[1],
                    pair[0]
                )
                sql = "SELECT * FROM stats_swaps \
                        WHERE started_at > ? \
                        AND started_at < ? \
                        AND maker_coin_ticker=? \
                        AND taker_coin_ticker=? \
                        AND is_success=1 \
                        ORDER BY started_at DESC;"
                self.sql_cursor.execute(
                    sql,
                    t,
                )
            else:
                t = (
                    start_time,
                    end_time,
                    pair[1],
                    pair[0],
                    limit
                )
                sql = "SELECT * FROM stats_swaps WHERE \
                        started_at > ? \
                        AND started_at < ? \
                        AND maker_coin_ticker=? \
                        AND taker_coin_ticker=? \
                        AND is_success=1 \
                        ORDER BY started_at DESC \
                        LIMIT ?;"
                self.sql_cursor.execute(
                    sql,
                    t,
                )
            data = self.sql_cursor.fetchall()
            swaps_for_pair_b_a = [dict(row) for row in data]
            for swap in swaps_for_pair_b_a:
                temp_maker_amount = swap["maker_amount"]
                swap["maker_amount"] = swap["taker_amount"]
                swap["taker_amount"] = temp_maker_amount
                swap["trade_type"] = "sell"
            swaps_for_pair = swaps_for_pair_a_b + swaps_for_pair_b_a
            return swaps_for_pair
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [get_swaps_for_pair]: {e}")
            return []

    def get_last_price_for_pair(self, base: str, quote: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()

        swap_price = None
        swap_time = None
        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{base}' \
                AND taker_coin_ticker='{quote}' AND is_success=1 \
                ORDER BY started_at DESC LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp = self.sql_cursor.fetchone()
        if resp is not None:
            swap_price = Decimal(resp["taker_amount"]) / Decimal(
                resp["maker_amount"]
            )
            swap_time = resp["started_at"]

        swap_price2 = None
        swap_time2 = None
        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{quote}' \
                AND taker_coin_ticker='{base}' AND is_success=1 \
                ORDER BY started_at DESC LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp2 = self.sql_cursor.fetchone()
        if resp2 is not None:
            swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                resp2["taker_amount"]
            )
            swap_time2 = resp2["started_at"]
        if swap_price and swap_price2:
            if swap_time > swap_time2:
                price = swap_price
                last_swap_time = swap_time
            else:
                price = swap_price2
                last_swap_time = swap_time2
        elif swap_price:
            price = swap_price
            last_swap_time = swap_time
        elif swap_price2:
            price = swap_price2
            last_swap_time = swap_time2
        else:
            price = 0
            last_swap_time = 0
        return {
            "price": price,
            "timestamp": last_swap_time,
        }


class Templates:
    def __init__(self):
        pass

    def gecko_info(self, coin_id):
        return {
            "usd_market_cap": 0,
            "usd_price": 0,
            "coingecko_id": coin_id
        }

    def pair_summary(self, base: str, quote: str):
        data = OrderedDict()
        data["trading_pair"] = f"{base}_{quote}"
        data["base_currency"] = base
        data["quote_currency"] = quote
        data["pair_swaps_count"] = 0
        data["base_price_usd"] = 0
        data["rel_price_usd"] = 0
        data["base_volume"] = 0
        data["rel_volume"] = 0
        data["base_liquidity_coins"] = 0
        data["base_liquidity_usd"] = 0
        data["base_trade_value_usd"] = 0
        data["rel_liquidity_coins"] = 0
        data["rel_liquidity_usd"] = 0
        data["rel_trade_value_usd"] = 0
        data["pair_liquidity_usd"] = 0
        data["pair_trade_value_usd"] = 0
        data["lowest_ask"] = 0
        data["highest_bid"] = 0
        return data

    def volumes_and_prices(self, suffix):
        return {
            "base_volume": 0,
            "quote_volume": 0,
            f"highest_price_{suffix}": 0,
            f"lowest_price_{suffix}": 0,
            "last_price": 0,
            "last_trade": 0,
            "trades_24hr": 0,
            f"price_change_percent_{suffix}": 0,
            f"price_change_{suffix}": 0,
        }

    def orderbook(self, base: str, quote: str, v2=False):
        data = {
            "pair": f"{base}_{quote}",
            "bids": [],
            "asks": [],
            "total_asks_base_vol": 0,
            "total_asks_rel_vol": 0,
            "total_bids_base_vol": 0,
            "total_bids_rel_vol": 0
        }
        if v2:  # pragma: no cover
            data.update({
                "total_asks_base_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_asks_rel_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_bids_base_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_bids_rel_vol": {
                    "decimal": 0
                }
            })
        return data


class Utils:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.files = Files()

    def get_db(self, db_path=const.MM2_DB_PATH, DB=None):
        if DB is not None:
            return DB
        return SqliteDB(db_path)

    def load_jsonfile(self, path, attempts=5):
        i = 0
        while True:
            i += 1
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except Exception as e:  # pragma: no cover
                if i >= attempts:
                    logger.error(f"{type(e)} Error loading {path}: {e}")
                    return None
                time.sleep(1)

    def download_json(self, url):
        try:
            return requests.get(url).json()
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error downloading {url}: {e}")
            return None

    def round_to_str(self, value: Any, rounding=8):
        try:
            if isinstance(value, (str, int, float)):
                value = Decimal(value)
            if isinstance(value, Decimal):
                value = value.quantize(Decimal(f'1.{"0" * rounding}'))
            else:
                raise TypeError(f"Invalid type: {type(value)}")
        except (ValueError, TypeError, InvalidOperation) as e:  # pragma: no cover
            logger.debug(f"{type(e)} Error rounding {value}: {e}")
            value = 0
        return f"{value:.{rounding}f}"

    def clean_decimal_dict_list(self, data, to_string=False, rounding=8):
        '''
        Works for a list of dicts with no nesting
        (e.g. summary_cache.json)
        '''
        for i in data:
            for j in i:
                if isinstance(i[j], Decimal):
                    if to_string:
                        i[j] = self.round_to_str(i[j], rounding)
                    else:
                        i[j] = round(float(i[j]), rounding)
        return data

    def clean_decimal_dict(self, data, to_string=False, rounding=8):
        '''
        Works for a simple dict with no nesting
        (e.g. summary_cache.json)
        '''
        for i in data:
            if isinstance(data[i], Decimal):
                if to_string:
                    data[i] = self.round_to_str(data[i], rounding)
                else:
                    data[i] = float(data[i])
        return data

    def get_suffix(self, days: int) -> str:
        if days == 1:
            return "24h"
        else:
            return f"{days}d"

    def get_related_coins(self, coin, exclude_segwit=True):
        try:
            coin = coin.split("-")[0]
            coins = self.load_jsonfile(self.files.coins)
            data = [
                i["coin"] for i in coins
                if i["coin"] == coin
                or i["coin"].startswith(f"{coin}-")
            ]
            if exclude_segwit:
                data = [i for i in data if "-segwit" not in i]
            return data
        except Exception as e:  # pragma: no cover
            logger.error(
                f"{type(e)} Error getting related coins for {coin}: {e}")
            return []

    def get_related_pairs(self, pair: tuple):
        coin_a = pair.as_tuple[0]
        coin_b = pair.as_tuple[1]
        coins_a = self.get_related_coins(coin_a, exclude_segwit=True)
        coins_b = self.get_related_coins(coin_b, exclude_segwit=True)
        return [
            (i, j) for i in coins_a
            for j in coins_b
            if i != j
        ]

    def get_chunks(self, data, chunk_length):
        for i in range(0, len(data), chunk_length):
            yield data[i: i + chunk_length]

    def get_gecko_usd_price(self, coin: str, gecko_source) -> float:
        try:
            return Decimal(gecko_source[coin]["usd_price"])
        except KeyError:  # pragma: no cover
            return 0

    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        lowest = 0
        try:
            for ask in orderbook["asks"]:
                price = ask["price"]
                if not isinstance(ask["price"], Decimal):
                    if "decimal" in ask["price"]:
                        price = Decimal(ask["price"]["decimal"])
                    else:
                        price = Decimal(ask["price"])
                if lowest == 0:
                    lowest = price
                elif Decimal(price) < Decimal(lowest):
                    lowest = price
        except KeyError as e:  # pragma: no cover
            logger.error(e)
        return format_10f(Decimal(lowest))

    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        highest = 0
        try:
            for bid in orderbook["bids"]:
                price = bid["price"]
                if not isinstance(bid["price"], Decimal):
                    if "decimal" in bid["price"]:
                        price = Decimal(bid["price"]["decimal"])
                    else:
                        price = Decimal(bid["price"])
                if highest == 0:
                    highest = price
                elif Decimal(price) > Decimal(highest):
                    highest = price
        except KeyError as e:  # pragma: no cover
            logger.error(e)
        return format_10f(Decimal(highest))


class DexAPI:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.utils = Utils()
        self.files = Files(self.testing)
        self.templates = Templates()
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config)

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    def orderbook(self, pair):
        try:
            if isinstance(pair, str):
                pair = pair.split("_")
            base = pair[0]
            quote = pair[1]
            if self.testing:
                orderbook = f"{api_root_path}/tests/fixtures/orderbook/{base}_{quote}.json"
                return self.utils.load_jsonfile(orderbook)
            if base not in self.coins_config or quote not in self.coins_config:
                return self.templates.orderbook(base, quote, v2=True)
            if self.coins_config[base]["wallet_only"] or self.coins_config[quote]["wallet_only"]:
                return self.templates.orderbook(base, quote, v2=True)

            mm2_host = "http://127.0.0.1:7783"
            params = {
                "mmrpc": "2.0",
                "method": "orderbook",
                "params": {
                    "base": pair[0],
                    "rel": pair[1]
                },
                "id": 42
            }
            r = requests.post(mm2_host, json=params)
            if "result" in json.loads(r.text):
                return json.loads(r.text)["result"]
        except Exception as e:  # pragma: no cover
            logger.error(
                f"{type(e)} Error in [DexAPI.orderbook] for {pair}: {e}")
            logger.info(r.text)
        return self.templates.orderbook(base, quote, v2=True)
