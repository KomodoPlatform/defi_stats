#!/usr/bin/env python3
import time
import json
from logger import logger
from helper import sum_json_key, sum_json_key_10f, sort_dict_list, get_netid_filename
from enums import NetId
from pair import Pair
from generics import Files
from utils import Utils
from external import CoinGeckoAPI, FixerAPI, PriceServiceAPI
from db import get_sqlite_db
from const import COINS_CONFIG_URL, COINS_URL, MM2_DB_PATH_7777


class Cache:
    def __init__(self, testing: bool = False, path_to_db=MM2_DB_PATH_7777):
        self.path_to_db = path_to_db
        self.testing = testing
        self.utils = Utils(self.testing)
        self.files = Files(self.testing)
        self.load = self.Load(files=self.files, utils=self.utils)
        self.calc = self.Calc(
            path_to_db=self.path_to_db,
            load=self.load,
            testing=self.testing,
            utils=self.utils,
        )
        self.save = self.Save(
            path_to_db=self.path_to_db,
            calc=self.calc,
            testing=self.testing,
            files=self.files,
            utils=self.utils,
        )
        # Coins repo data
        self.coins_cache = None
        self.coins_config_cache = None
        # For CoinGecko endpoints
        self.gecko_source_cache = None
        self.gecko_tickers_cache = None
        self.prices_tickers_v1_cache = None
        self.prices_tickers_v2_cache = None
        self.refresh()

    def refresh(self):
        # Coins repo data
        self.coins_cache = self.load.load_coins()
        self.coins_config_cache = self.load.load_coins_config()
        # For CoinGecko endpoints
        self.gecko_source_cache = self.load.load_gecko_source()
        for netid in NetId:
            self.gecko_pairs_cache = self.load.load_gecko_pairs(netid=netid.value)
            self.gecko_tickers_cache = self.load.load_gecko_tickers(netid=netid.value)
            self.markets_pairs_cache = self.load.load_markets_pairs(netid=netid.value)
            self.markets_tickers_cache = self.load.load_markets_tickers(
                netid=netid.value
            )
            self.markets_last_trade_cache = self.load.load_markets_last_trade(
                netid=netid.value
            )
        # For Rates endpoints
        self.fixer_rates_cache = self.load.load_fixer_rates()
        # For Prices endpoints
        self.prices_tickers_v1_cache = self.load.load_prices_tickers_v1()
        self.prices_tickers_v2_cache = self.load.load_prices_tickers_v2()

    class Load:
        def __init__(self, files, utils):
            self.files = files
            self.utils = utils

        # Coins repo data
        def load_coins(self):
            return self.utils.load_jsonfile(self.files.coins_file)

        def load_coins_config(self):
            return self.utils.load_jsonfile(self.files.coins_config_file)

        # For CoinGecko endpoints
        def load_gecko_source(self):
            return self.utils.load_jsonfile(self.files.gecko_source_file)

        def load_gecko_tickers(self, netid):
            fn = get_netid_filename(self.files.gecko_tickers_file, netid)
            return self.utils.load_jsonfile(fn)

        def load_gecko_pairs(self, netid):
            fn = get_netid_filename(self.files.gecko_pairs_file, netid)
            return self.utils.load_jsonfile(fn)

        # For Markets endpoints
        def load_markets_tickers(self, netid):
            fn = get_netid_filename(self.files.markets_tickers_file, netid)
            return self.utils.load_jsonfile(fn)

        def load_markets_pairs(self, netid):
            fn = get_netid_filename(self.files.markets_pairs_file, netid)
            return self.utils.load_jsonfile(fn)

        def load_markets_last_trade(self, netid):
            fn = get_netid_filename(self.files.markets_last_trade_file, netid)
            return self.utils.load_jsonfile(fn)

        # For Rates endpoints
        def load_fixer_rates(self):
            return self.utils.load_jsonfile(self.files.fixer_rates_file)

        # For Prices endpoints
        def load_prices_tickers_v1(self):
            return self.utils.load_jsonfile(self.files.prices_tickers_v1_file)

        def load_prices_tickers_v2(self):
            return self.utils.load_jsonfile(self.files.prices_tickers_v2_file)

    class Calc:
        def __init__(self, path_to_db, load, testing, utils):
            self.path_to_db = path_to_db
            self.load = load
            self.testing = testing
            self.utils = utils
            self.gecko = CoinGeckoAPI(self.testing)
            self.fixer = FixerAPI(self.testing)
            self.price_service = PriceServiceAPI(self.testing)

        # For Rates endpoints
        def calc_fixer_rates_source(self):
            return self.fixer.get_fixer_rates_source()

        # For Prices endpoints
        def calc_prices_tickers_v1(self):
            return self.price_service.get_calc_prices_tickers_v1()

        def calc_prices_tickers_v2(self):
            return self.price_service.get_calc_prices_tickers_v2()

        # For CoinGecko endpoints
        def calc_gecko_source(self):
            return self.gecko.get_gecko_source()

        def is_pair_priced(self, pair: tuple) -> bool:
            """
            Checks if both coins in a pair are priced.
            """
            try:
                base = pair[0].split("-")[0]
                rel = pair[1].split("-")[0]
                common = set((base, rel)).intersection(self.gecko.priced_coins)
                return len(common) == 2
            except Exception as e:  # pragma: no cover
                err = {"error": f"{type(e)} Error checking if {pair} is priced: {e}"}
                logger.error(err)
                return False

        def calc_gecko_pairs(
            self, days: int = 7, exclude_unpriced: bool = True, DB=None, netid=7777
        ) -> list:
            DB = get_sqlite_db(path_to_db=self.path_to_db, testing=self.testing, DB=DB)
            try:
                pairs = DB.get_pairs(days)
                data = [
                    Pair(i, self.testing).info
                    for i in pairs
                    if self.is_pair_priced(i) or not exclude_unpriced
                ]
                data = sorted(data, key=lambda d: d["ticker_id"])
                logger.debug(
                    f"{len(data)} priced pairs ({days} days) days) from netid [{netid}]"
                )
                return data
            except Exception as e:  # pragma: no cover
                err = {"error": f"[calc_gecko_pairs]: {e}"}
                logger.error(err)
                return err  # pragma: no cover

        def calc_gecko_tickers(
            self, trades_days: int = 1, pairs_days: int = 7, DB=None, netid=7777
        ):
            DB = get_sqlite_db(path_to_db=self.path_to_db, testing=self.testing, DB=DB)
            pairs = DB.get_pairs(pairs_days)
            logger.debug(
                f"[gecko_tickers] {len(pairs)} pairs ({pairs_days} days) from netid [{netid}]"
            )
            data = [
                Pair(i, self.testing).gecko_ticker_info(trades_days, DB=DB)
                for i in pairs
            ]
            # Remove None values (from coins without price)
            data = [i for i in data if "ticker_id" in i]
            data = self.utils.clean_decimal_dict_list(data, to_string=True, rounding=10)
            data = sort_dict_list(data, "ticker_id")
            return {
                "last_update": int(time.time()),
                "pairs_count": len(data),
                "swaps_count": int(sum_json_key(data, "trades_24hr")),
                "combined_volume_usd": sum_json_key_10f(data, "volume_usd_24hr"),
                "combined_liquidity_usd": sum_json_key_10f(data, "liquidity_in_usd"),
                "data": data,
            }

        def calc_markets_pairs(
            self, days: int = 7, exclude_unpriced: bool = True, DB=None, netid=7777
        ) -> list:
            return self.calc_gecko_pairs(DB=DB, days=180, netid=netid)

        def calc_markets_tickers(
            self, trades_days: int = 1, pairs_days: int = 7, DB=None, netid=7777
        ):
            return self.calc_gecko_tickers(DB=DB, pairs_days=180, netid=netid)

        def calc_markets_last_trade(self, DB=None, netid=7777):
            return DB.get_pairs_last_trade(min_swaps=1, as_dict=True)

    class Save:
        """
        Updates cache json files.
        """

        def __init__(
            self, calc, files, utils, testing=False, path_to_db=MM2_DB_PATH_7777
        ):
            self.calc = calc
            self.files = files
            self.testing = testing
            self.utils = utils
            self.path_to_db = path_to_db

        def save(self, path, data):
            if not isinstance(data, (dict, list)):
                raise TypeError(
                    f"Invalid data type: {type(data)}, must be dict or list"
                )
            elif "error" in data:
                raise Exception(data["error"])
            elif self.testing:  # pragma: no cover
                if path in [self.files.gecko_source_file, self.files.coins_config_file]:
                    return {"result": f"Validated {path} data"}
            with open(path, "w+") as f:
                json.dump(data, f, indent=4)
                logger.info(f"Updated {path}")
                return {"result": f"Updated {path}"}

        # Coins repo data
        def save_coins_config(self, url=COINS_CONFIG_URL):
            data = self.utils.download_json(url)
            if data is not None:
                return self.save(self.files.coins_config_file, data)

        def save_coins(self, url=COINS_URL):
            data = self.utils.download_json(url)
            if data is not None:
                return self.save(self.files.coins_file, data)

        # For Rates endpoints
        def save_fixer_rates_source(self):  # pragma: no cover
            data = self.calc.calc_fixer_rates_source()
            return self.save(self.files.fixer_rates_file, data)

        # For Prices endpoints
        def save_prices_tickers_v1(self):
            data = self.calc.calc_prices_tickers_v1()
            return self.save(self.files.prices_tickers_v1_file, data)

        def save_prices_tickers_v2(self):
            data = self.calc.calc_prices_tickers_v2()
            return self.save(self.files.prices_tickers_v2_file, data)

        # For CoinGecko endpoints
        def save_gecko_source(self):  # pragma: no cover
            data = self.calc.calc_gecko_source()
            if "error" in data:
                logger.warning(data["error"])
            else:
                self.save(self.files.gecko_source_file, data)

        def save_gecko_pairs(self, netid, DB=None):  # pragma: no cover
            DB = get_sqlite_db(
                path_to_db=self.path_to_db, testing=self.testing, DB=DB, netid=netid
            )
            data = self.calc.calc_gecko_pairs(DB=DB, netid=netid)
            fn = get_netid_filename(self.files.gecko_pairs_file, netid)
            return self.save(fn, data)

        def save_gecko_tickers(self, netid, DB=None):  # pragma: no cover
            DB = get_sqlite_db(
                path_to_db=self.path_to_db, testing=self.testing, DB=DB, netid=netid
            )
            data = self.calc.calc_gecko_tickers(DB=DB, netid=netid)
            fn = get_netid_filename(self.files.gecko_tickers_file, netid)
            return self.save(fn, data)

        # For Markets endpoints
        def save_markets_pairs(self, netid, DB=None):  # pragma: no cover
            DB = get_sqlite_db(
                path_to_db=self.path_to_db, testing=self.testing, DB=DB, netid=netid
            )
            data = self.calc.calc_markets_pairs(DB=DB, days=180, netid=netid)
            fn = get_netid_filename(self.files.markets_pairs_file, netid)
            return self.save(fn, data)

        def save_markets_last_trade(self, netid, DB=None):  # pragma: no cover
            DB = get_sqlite_db(
                path_to_db=self.path_to_db, testing=self.testing, DB=DB, netid=netid
            )
            data = self.calc.calc_markets_last_trade(DB=DB, netid=netid)
            fn = get_netid_filename(self.files.markets_last_trade_file, netid)
            return self.save(fn, data)

        def save_markets_tickers(self, netid, DB=None):  # pragma: no cover
            DB = get_sqlite_db(
                path_to_db=self.path_to_db, testing=self.testing, DB=DB, netid=netid
            )
            data = self.calc.calc_markets_tickers(DB=DB, pairs_days=180, netid=netid)
            fn = get_netid_filename(self.files.markets_tickers_file, netid)
            return self.save(fn, data)
