#!/usr/bin/env python3
import time
from util.exceptions import NoDefaultForKeyError


class Templates:
    def __init__(self, mm2_host="http://127.0.0.1"):
        self.mm2_host = mm2_host

    def gecko_info(self, coin_id):
        return {"usd_market_cap": 0, "usd_price": 0, "coingecko_id": coin_id}

    def last_price_for_pair(self):
        return {"timestamp": 0, "price": 0}

    def swap_counts(self):
        return {"swaps_all_time": 0, "swaps_30d": 0, "swaps_24h": 0}

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
            "base": base,
            "quote": quote,
            "bids": [],
            "asks": [],
            "total_asks_base_vol": 0,
            "total_asks_quote_vol": 0,
            "total_bids_base_vol": 0,
            "total_bids_quote_vol": 0,
        }
        if v2:  # pragma: no cover
            data.update({"total_asks_base_vol": {"decimal": 0}})
            data.update({"total_asks_quote_vol": {"decimal": 0}})
            data.update({"total_bids_base_vol": {"decimal": 0}})
            data.update({"total_bids_quote_vol": {"decimal": 0}})
        return data

    @property
    def arg_defaults(self):
        val_keys = {
            "false": [
                "testing",
                "include_all_kmd",
                "context",
                "updated",
                "query",
                "imported",
                "error",
                "warning",
                "debug",
                "dexrpc",
                "muted",
                "info",
                "loop",
                "calc",
                "save",
                "request"
            ],
            "true": ["wal", "exclude_unpriced", "dict_format"],
            "none": ["endpoint", "reverse", "source_url"],
            "now": ["end"],
            "all": ["netid"],
            "default_host": ["mm2_host"],
            "zero": ["trigger"],
            "debug": ["loglevel"],
        }
        args = []
        for v in val_keys.values():
            args += v
        return {"val_keys": val_keys, "args": args}

    def default_val(self, key: str):
        for val, keys in self.arg_defaults["val_keys"].items():
            if key in keys:
                if val.lower() == "true":
                    return True
                if val.lower() == "false":
                    return False
                if val.lower() == "none":
                    return None
                if val.lower() == "all":
                    return "ALL"
                if val.lower() == "empty_string":
                    return ""
                if val.lower() == "zero":
                    return 0
                if val.lower() == "default_host":
                    return self.mm2_host
                if val.lower() == "now":
                    return int(time.time())
                if val.lower() == "debug":
                    return "debug"
        raise NoDefaultForKeyError(f"No default value for {key}!")

    def set_params(self, object: object(), kwargs: dict(), options: list()) -> None:
        # Set the defaults from object options if not already set
        for arg in self.arg_defaults["args"]:
            if arg in options:
                if getattr(object, arg, "unset") == "unset":
                    setattr(object, arg, self.default_val(arg))
        # Then process kwargs
        [setattr(object, k, v) for k, v in kwargs.items()]

def default_error(e, msg, loglevel='error'):
    r = {
        "result": "error",
        "message": f"{type(e)}: {e}",
        "loglevel": loglevel
    }
    if msg is not None:
        r.update({"message": msg})
    return r

def default_result(msg, loglevel='debug'):
    r = {
        "result": "success",
        "message": msg,
        "loglevel": loglevel
    }
    if msg is not None:
        r.update({"message": msg})
    return r