from decimal import Decimal, InvalidOperation
from util.logger import logger
from typing import Any
from util.defaults import default_error


def ticker_to_market_ticker_summary(i):
    return {
        "trading_pair": f"{i['base_currency']}_{i['target_currency']}",
        "last_price": i["last_price"],
        "base_currency": i["base_currency"],
        "base_volume": i["base_volume"],
        "quote_currency": i["target_currency"],
        "quote_volume": i["target_volume"],
        "lowest_ask": i["ask"],
        "highest_bid": i["bid"],
        "price_change_percent_24h": str(i["price_change_percent_24h"]),
        "highest_price_24h": i["high"],
        "lowest_price_24h": i["low"],
        "trades_24h": int(i["trades_24hr"]),
        "last_swap_timestamp": int(i["last_trade"]),
    }


def ticker_to_market_ticker(i):
    return {
        f"{i['base_currency']}_{i['target_currency']}": {
            "last_price": i["last_price"],
            "quote_volume": i["target_volume"],
            "base_volume": i["base_volume"],
            "isFrozen": "0",
        }
    }


def historical_trades_to_market_trades(i):
    return {
        "trade_id": i["trade_id"],
        "price": i["price"],
        "base_volume": i["base_volume"],
        "quote_volume": i["target_volume"],
        "timestamp": i["timestamp"],
        "type": i["type"],
    }


def round_to_str(value: Any, rounding=8):
    try:
        if isinstance(value, (str, int, float)):
            value = Decimal(value)
        if isinstance(value, Decimal):
            value = value.quantize(Decimal(f'1.{"0" * rounding}'))
        else:
            raise TypeError(f"Invalid type: {type(value)}")
    except (ValueError, TypeError, InvalidOperation) as e:  # pragma: no cover
        logger.muted(f"{type(e)} Error rounding {value}: {e}")
        value = 0
    except Exception as e:  # pragma: no cover
        logger.error(e)
        value = 0
    return f"{value:.{rounding}f}"


def clean_decimal_dict_list(data, to_string=False, rounding=8):
    """
    Works for a list of dicts with no nesting
    (e.g. summary_cache.json)
    """
    try:
        for i in data:
            for j in i:
                if isinstance(i[j], Decimal):
                    if to_string:
                        i[j] = round_to_str(i[j], rounding)
                    else:
                        i[j] = round(float(i[j]), rounding)
        return data
    except Exception as e:  # pragma: no cover
        return default_error(e)


def clean_decimal_dict(data, to_string=False, rounding=8):
    """
    Works for a simple dict with no nesting
    (e.g. summary_cache.json)
    """
    try:
        for i in data:
            if isinstance(data[i], Decimal):
                if to_string:
                    data[i] = round_to_str(data[i], rounding)
                else:
                    data[i] = float(data[i])
        return data
    except Exception as e:  # pragma: no cover
        return default_error(e)


def get_suffix(days: int) -> str:
    if days == 1:
        return "24h"
    else:
        return f"{days}d"


def format_10f(number: float) -> str:
    """
    Format a float to 10 decimal places.
    """
    return f"{number:.10f}"


def list_json_key(data: dict, key: str, filter_value: str) -> Decimal:
    """
    list of key values from dicts.
    """
    return [i for i in data if i[key] == filter_value]


def sum_json_key(data: dict, key: str) -> Decimal:
    """
    Sum a key from a list of dicts.
    """
    return sum(Decimal(d[key]) for d in data)


def sum_json_key_10f(data: dict, key: str) -> str:
    """
    Sum a key from a list of dicts and format to 10 decimal places.
    """
    return format_10f(sum_json_key(data, key))


def sort_dict_list(data: list(), key: str, reverse=False) -> dict:
    """
    Sort a list of dicts by the value of a key.
    """
    return sorted(data, key=lambda k: k[key], reverse=reverse)


def sort_dict(data: dict, reverse=False) -> dict:
    """
    Sort a dict by the value the root key.
    """
    k = list(data.keys())
    k.sort()
    if reverse:
        k.reverse()
    resp = {}
    for i in k:
        resp.update({i: data[i]})
    return resp


def merge_orderbooks(existing, new):
    existing["asks"] += new["asks"]
    existing["bids"] += new["bids"]
    existing["liquidity_usd"] += new["liquidity_usd"]
    existing["total_asks_base_vol"] += new["total_asks_base_vol"]
    existing["total_bids_base_vol"] += new["total_bids_base_vol"]
    existing["total_asks_quote_vol"] += new["total_asks_quote_vol"]
    existing["total_bids_quote_vol"] += new["total_bids_quote_vol"]
    existing["total_asks_base_usd"] += new["total_asks_base_usd"]
    existing["total_bids_quote_usd"] += new["total_bids_quote_usd"]
    return existing
