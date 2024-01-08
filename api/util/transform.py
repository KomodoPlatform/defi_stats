from decimal import Decimal, InvalidOperation
from util.logger import logger, timed
from typing import Any, List
from util.defaults import default_error
import lib


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
        return "24hr"
    else:
        return f"{days}d"


def format_10f(number: float) -> str:
    """
    Format a float to 10 decimal places.
    """
    if isinstance(number, str):
        number = float(number)
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


def sort_dict_list(data: List, key: str, reverse=False) -> dict:
    """
    Sort a list of dicts by the value of a key.
    """
    resp = sorted(data, key=lambda k: k[key], reverse=reverse)
    return resp


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


def get_top_items(data, sort_key, length=5):
    data.sort(key=lambda x: x[sort_key], reverse=True)
    return data[:length]


@timed
def order_pair_by_market_cap(pair_str: str, gecko_source=None, testing=False) -> str:
    try:
        pair_list = pair_str.split("_")
        base = pair_list[0]
        quote = pair_list[1]
        base_price, base_mc = lib.get_gecko_price_and_mcap(
            base, gecko_source=gecko_source, testing=testing
        )
        quote_price, quote_mc = lib.get_gecko_price_and_mcap(
            quote, gecko_source=gecko_source, testing=testing
        )
        if quote_mc < base_mc:
            pair_str = reverse_ticker(pair_str)
        elif quote_mc == base_mc:
            pair_str = "_".join(sorted(pair_list))
    except Exception as e:  # pragma: no cover
        msg = f"order_pair_by_market_cap failed: {e}"
        logger.warning(msg)
    return pair_str


def merge_orderbooks(existing, new):
    try:
        existing["asks"] += new["asks"]
        existing["bids"] += new["bids"]
        existing["liquidity_usd"] += new["liquidity_usd"]
        existing["total_asks_base_vol"] += new["total_asks_base_vol"]
        existing["total_bids_base_vol"] += new["total_bids_base_vol"]
        existing["total_asks_quote_vol"] += new["total_asks_quote_vol"]
        existing["total_bids_quote_vol"] += new["total_bids_quote_vol"]
        existing["total_asks_base_usd"] += new["total_asks_base_usd"]
        existing["total_bids_quote_usd"] += new["total_bids_quote_usd"]
        existing["trades_24hr"] += new["trades_24hr"]
        existing["volume_usd_24hr"] += new["volume_usd_24hr"]

    except Exception as e:  # pragma: no cover
        err = {"error": f"transform.merge_orderbooks: {e}"}
        logger.warning(err)
    return existing


def orderbook_to_gecko(data):
    bids = [[i["price"], i["volume"]] for i in data["bids"]]
    asks = [[i["price"], i["volume"]] for i in data["asks"]]
    data["asks"] = asks
    data["bids"] = bids
    data["ticker_id"] = data["pair"]
    return data


def to_summary_for_ticker_item(data):  # pragma: no cover
    return {
        "pair": data["ticker_id"],
        "base": data["base_currency"],
        "liquidity_usd": data["liquidity_in_usd"],
        "base_volume": data["base_volume"],
        "base_usd_price": data["base_usd_price"],
        "quote": data["target_currency"],
        "quote_volume": data["target_volume"],
        "quote_usd_price": data["target_usd_price"],
        "highest_bid": data["bid"],
        "lowest_ask": data["ask"],
        "highest_price_24hr": data["high"],
        "lowest_price_24hr": data["low"],
        "price_change_24hr": data["price_change_24hr"],
        "price_change_percent_24hr": data["price_change_percent_24hr"],
        "trades_24hr": data["trades_24hr"],
        "volume_usd_24hr": data["volume_usd_24hr"],
        "last_price": data["last_price"],
        "last_trade": data["last_trade"],
    }


def ticker_to_market_ticker_summary(i):
    return {
        "trading_pair": f"{i['base_currency']}_{i['target_currency']}",
        "base_currency": i["base_currency"],
        "base_volume": i["base_volume"],
        "quote_currency": i["target_currency"],
        "quote_volume": i["target_volume"],
        "lowest_ask": i["ask"],
        "highest_bid": i["bid"],
        "price_change_percent_24hr": str(i["price_change_percent_24hr"]),
        "highest_price_24hr": i["high"],
        "lowest_price_24hr": i["low"],
        "trades_24hr": int(i["trades_24hr"]),
        "last_swap": int(i["last_trade"]),
        "last_price": i["last_price"],
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


def ticker_to_gecko(i):
    return {
        "ticker_id": i["ticker_id"],
        "pool_id": i["ticker_id"],
        "base_currency": i["base_currency"],
        "target_currency": i["target_currency"],
        "bid": i["bid"],
        "ask": i["ask"],
        "high": i["high"],
        "low": i["low"],
        "base_volume": i["base_volume"],
        "target_volume": i["target_volume"],
        "last_price": i["last_price"],
        "last_trade": i["last_trade"],
        "trades_24hr": i["trades_24hr"],
        "volume_usd_24hr": i["volume_usd_24hr"],
        "liquidity_in_usd": i["liquidity_in_usd"],
    }


def ticker_to_statsapi(i, suffix):
    try:
        if suffix == "24hr":
            alt_suffix = "24h"
        else:
            alt_suffix = suffix
        return {
            "trading_pair": i["ticker_id"],
            "pair_swaps_count": int(i[f"trades_{suffix}"]),
            "pair_liquidity_usd": Decimal(i["liquidity_in_usd"]),
            "pair_trade_value_usd": Decimal(i[f"volume_usd_{suffix}"]),
            "base_currency": i["base_currency"],
            "base_volume": Decimal(i["base_volume"]),
            "base_price_usd": Decimal(i["base_usd_price"]),
            "base_trade_value_usd": Decimal(i["base_volume_usd"]),
            "base_liquidity_coins": Decimal(i["base_liquidity_coins"]),
            "base_liquidity_usd": Decimal(i["base_liquidity_usd"]),
            "quote_currency": i["target_currency"],
            "quote_volume": Decimal(i["target_volume"]),
            "quote_price_usd": Decimal(i["target_usd_price"]),
            "quote_trade_value_usd": Decimal(i["quote_volume_usd"]),
            "quote_liquidity_coins": Decimal(i["quote_liquidity_coins"]),
            "quote_liquidity_usd": Decimal(i["quote_liquidity_usd"]),
            "highest_bid": Decimal(i["bid"]),
            "lowest_ask": Decimal(i["ask"]),
            f"highest_price_{alt_suffix}": Decimal(i["high"]),
            f"lowest_price_{alt_suffix}": Decimal(i["low"]),
            f"price_change_{alt_suffix}": Decimal(i[f"price_change_{suffix}"]),
            f"price_change_percent_{alt_suffix}": Decimal(
                i[f"price_change_percent_{suffix}"]
            ),
            "last_trade": int(i["last_trade"]),
            "last_price": Decimal(i["last_price"]),
        }
    except Exception as e:  # pragma: no cover
        return default_error(e)


def historical_trades_to_market_trades(i):
    return {
        "trade_id": i["trade_id"],
        "price": i["price"],
        "base_volume": i["base_volume"],
        "quote_volume": i["target_volume"],
        "timestamp": i["timestamp"],
        "type": i["type"],
    }


def historical_trades_to_gecko(i):
    return {
        "trade_id": i["trade_id"],
        "price": i["price"],
        "base_volume": i["base_volume"],
        "target_volume": i["target_volume"],
        "timestamp": i["timestamp"],
        "type": i["type"],
    }


def reverse_ticker(ticker_id):
    return "_".join(ticker_id.split("_")[::-1])


def pairs_to_gecko(generic_data):
    # Remove unpriced
    return [i for i in generic_data if i["priced"]]
