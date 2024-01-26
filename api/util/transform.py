from decimal import Decimal, InvalidOperation
from util.logger import logger, timed
from typing import Any, List, Dict
import util.defaults as default
import util.memcache as memcache


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
        return default.error(e)


def clean_decimal_dict(data, to_string=False, rounding=8, exclude_keys: List = list()):
    """
    Works for a simple dict with no nesting
    (e.g. summary_cache.json)
    """
    try:
        for i in data:
            if i not in exclude_keys:
                if isinstance(data[i], Decimal):
                    if to_string:
                        data[i] = round_to_str(data[i], rounding)
                    else:
                        data[i] = float(data[i])
        return data
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_suffix(days: int) -> str:
    if days == 1:
        return "24hr"
    else:
        return f"{days}d"


def format_10f(number: float | Decimal) -> str:
    """
    Format a float to 10 decimal places.
    """
    if isinstance(number, str):
        number = Decimal(number)
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


def get_top_items(data: List[Dict], sort_key: str, length: int = 5):
    data.sort(key=lambda x: x[sort_key], reverse=True)
    return data[:length]


@timed
def order_pair_by_market_cap(pair_str: str, gecko_source=None) -> str:
    try:
        if gecko_source is None:
            gecko_source = memcache.get_gecko_source()
        if gecko_source is not None:
            pair_str.replace("-segwit", "")
            pair_list = pair_str.split("_")
            base = pair_list[0]
            quote = pair_list[1]
            base_mc = 0
            quote_mc = 0
            if base in gecko_source:
                base_mc = Decimal(gecko_source[base]["usd_market_cap"])
            if quote in gecko_source:
                quote_mc = Decimal(gecko_source[quote]["usd_market_cap"])
            if quote_mc < base_mc:
                pair_str = invert_pair(pair_str)
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
        if "trades_24hr" in existing and "trades_24hr" in new:
            existing["trades_24hr"] += new["trades_24hr"]
        elif "trades_24hr" in new:
            existing["trades_24hr"] = new["trades_24hr"]
        else:
            existing["trades_24hr"] = 0
        if "volume_usd_24hr" in existing and "volume_usd_24hr" in new:
            existing["volume_usd_24hr"] += new["volume_usd_24hr"]
        elif "volume_usd_24hr" in new:
            existing["volume_usd_24hr"] = new["volume_usd_24hr"]
        else:
            existing["volume_usd_24hr"] = 0

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
        "price_change_pct_24hr": data["price_change_pct_24hr"],
        "trades_24hr": data["trades_24hr"],
        "volume_usd_24hr": data["volume_usd_24hr"],
        "last_price": data["last_price"],
        "last_trade": data["last_trade"],
    }


def to_summary_for_ticker_xyz_item(data):  # pragma: no cover
    return {
        "trading_pair": data["ticker_id"],
        "base_currency": data["base_currency"],
        "liquidity_usd": data["liquidity_in_usd"],
        "base_volume": data["base_volume"],
        "base_usd_price": data["base_usd_price"],
        "quote_currency": data["target_currency"],
        "quote_volume": data["target_volume"],
        "quote_usd_price": data["target_usd_price"],
        "highest_bid": data["bid"],
        "lowest_ask": data["ask"],
        "highest_price_24h": data["high"],
        "lowest_price_24h": data["low"],
        "price_change_24h": data["price_change_24hr"],
        "price_change_pct_24h": data["price_change_pct_24hr"],
        "trades_24h": data["trades_24hr"],
        "volume_usd_24h": data["volume_usd_24hr"],
        "last_price": data["last_price"],
        "last_swap_timestamp": data["last_trade"],
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
        "price_change_pct_24hr": str(i["price_change_pct_24hr"]),
        "highest_price_24hr": i["high"],
        "lowest_price_24hr": i["low"],
        "trades_24hr": int(i["trades_24hr"]),
        "last_swap": int(i["last_trade"]),
        "last_price": i["last_price"],
    }


def ticker_to_xyz_summary(i):
    return {
        "trading_pair": f"{i['base_currency']}_{i['target_currency']}",
        "base_currency": i["base_currency"],
        "base_volume": i["base_volume"],
        "quote_currency": i["target_currency"],
        "quote_volume": i["target_volume"],
        "lowest_ask": i["ask"],
        "last_swap_timestamp": int(i["last_trade"]),
        "highest_bid": i["bid"],
        "price_change_pct_24h": str(i["price_change_pct_24hr"]),
        "highest_price_24h": i["high"],
        "lowest_price_24h": i["low"],
        "trades_24h": int(i["trades_24hr"]),
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
    data = {
        "ticker_id": strip_pair_platforms(i["ticker_id"]),
        "pool_id": strip_pair_platforms(i["ticker_id"]),
        "base_currency": strip_coin_platform(i["base_currency"]),
        "target_currency": strip_coin_platform(i["target_currency"]),
        "bid": format_10f(i["bid"]),
        "ask": format_10f(i["ask"]),
        "high": format_10f(i["high"]),
        "low": format_10f(i["low"]),
        "base_volume": format_10f(i["base_volume"]),
        "target_volume": format_10f(i["target_volume"]),
        "last_price": format_10f(i["last_price"]),
        "last_trade": int(Decimal(i["last_trade"])),
        "trades_24hr": int(Decimal(i["trades_24hr"])),
        "volume_usd_24hr": format_10f(i["volume_usd_24hr"]),
        "liquidity_in_usd": format_10f(i["liquidity_in_usd"]),
    }

    if "last_swap_uuid" in i:
        data.update({"last_swap_uuid": i["last_swap_uuid"]})
    if "variants" in i:
        data.update({"variants": i["variants"]})
    return data


@timed
def ticker_to_statsapi_summary(i, suffix):
    try:
        if suffix == "24hr":
            alt_suffix = "24h"
        else:
            alt_suffix = suffix
        data = {
            "trading_pair": i["ticker_id"],
            "pair_swaps_count": int(i[f"trades_{suffix}"]),
            "pair_liquidity_usd": Decimal(i["liquidity_in_usd"]),
            "pair_trade_value_usd": Decimal(i[f"combined_volume_usd"]),
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
            "newest_price": i["newest_price"],
            "oldest_price": i["oldest_price"],
            "newest_price_time": i["newest_price_time"],
            "oldest_price_time": i["oldest_price_time"],
            "highest_bid": Decimal(i["bid"]),
            "lowest_ask": Decimal(i["ask"]),
            f"highest_price_{alt_suffix}": Decimal(i["high"]),
            f"lowest_price_{alt_suffix}": Decimal(i["low"]),
            f"price_change_{alt_suffix}": Decimal(i[f"price_change_{suffix}"]),
            f"price_change_pct_{alt_suffix}": Decimal(i[f"price_change_pct_{suffix}"]),
            "last_swap_price": i["last_swap_price"],
            "last_swap_time": i["last_swap_time"],
            "last_swap_uuid": i["last_swap_uuid"],
            "variants": i["variants"]
        }
        return data

    except Exception as e:  # pragma: no cover
        return default.error(e)


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


def strip_pair_platforms(pair):
    coins = pair.split("_")
    return f"{strip_coin_platform(coins[0])}_{strip_coin_platform(coins[1])}"


def strip_coin_platform(coin):
    return coin.split("-")[0]


def get_coin_platform(coin):
    r = coin.split("-")
    if len(r) == 2:
        return r[1]
    return ""


def deplatform_pair_summary_item(i):
    resp = {}
    keys = i.keys()
    for k in keys:
        if k == "trading_pair":
            resp.update({k: strip_pair_platforms(i[k])})
        elif k in ["base_currency", "quote_currency"]:
            resp.update({k: strip_coin_platform(i[k])})
        else:
            resp.update({k: i[k]})
    return resp


def traded_cache_to_stats_api(traded_cache):
    resp = {}
    for i in traded_cache:
        cleaned_ticker = strip_pair_platforms(i)
        if cleaned_ticker not in resp:
            resp.update({cleaned_ticker: traded_cache[i]})
        else:
            if (
                resp[cleaned_ticker]["last_swap_time"]
                < traded_cache[i]["last_swap_time"]
            ):
                resp.update({cleaned_ticker: traded_cache[i]})
            elif (
                resp[cleaned_ticker]["first_swap_time"]
                > traded_cache[i]["first_swap_time"]
            ):
                resp.update({cleaned_ticker: traded_cache[i]})
    return resp


def invert_pair(ticker_id):
    return "_".join(ticker_id.split("_")[::-1])


def pairs_to_gecko(generic_data):
    # Remove unpriced
    return [i for i in generic_data if i["priced"]]


def update_if_greater(existing, new, key, secondary_key=None):
    if existing[key] < new[key]:
        existing[key] = new[key]
        if secondary_key is not None:
            existing[secondary_key] = new[secondary_key]


def update_if_lesser(existing, new, key, secondary_key=None):
    if existing[key] > new[key]:
        existing[key] = new[key]
        if secondary_key is not None:
            existing[secondary_key] = new[secondary_key]


def invert_trade_type(trade_type):
    if trade_type == "buy":
        return "sell"
    if trade_type == "sell":
        return "buy"
    return trade_type


def derive_app(appname):
    logger.query(f"appname: {appname}")
    gui, match = derive_gui(appname)
    appname.replace(match, "")
    app_version, match = derive_app_version(appname)
    appname.replace(match, "")
    defi_version, match = derive_defi_version(appname)
    appname.replace(match, "")
    # check the last to avoid false positives: e.g. web / web_dex
    device, match = derive_device(appname)
    appname.replace(match, "")
    derived_app = f"{gui} {app_version} {device} (sdk: {defi_version})"
    logger.calc(f"appname remaining: {appname}")
    logger.info(f"derived_app: {derived_app}")
    return derived_app


def derive_gui(appname):
    for i in DeFiApps:
        for j in DeFiApps[i]:
            if j in appname.lower():
                return i, j
    return "Unknown", ""


def derive_device(appname):
    for i in DeFiDevices:
        for j in DeFiDevices[i]:
            if j in appname.lower():
                return i, j
    return "Unknown", ""


def derive_app_version(appname):
    parts = appname.split(" ")
    for i in parts:
        subparts = i.split("-")
        for j in subparts:
            version_parts = j.split(".")
            for k in version_parts:
                try:
                    int(k)
                except ValueError:
                    break
                except Exception as e:
                    logger.warning(e)
                    break
                return j, j
    return "Unknown", ""


def derive_defi_version(appname):
    parts = appname.split("_")
    for i in parts:
        if len(i) > 6:
            try:
                int(i, 16)
                return i, i
            except ValueError:
                pass
    return "Unknown", ""


DeFiApps = {
    "Adex-CLI": ["adex-cli"],
    "AirDex": ["air_dex", "airdex"],
    "AtomicDEX": ["atomicdex"],
    "BitcoinZ Dex": ["bitcoinz dex"],
    "BumbleBee": ["bumblebee"],
    "CLI": ["cli", "atomicdex client cli"],
    "ColliderDex": ["colliderdex desktop"],
    "DexStats": ["dexstats"],
    "Docs Walkthru": [
        "docs_walkthru",
        "devdocs",
        "core_readme",
        "kmd_atomicdex_api_tutorial",
    ],
    "DogeDex": ["dogedex"],
    "Faucet": ["faucet"],
    "FiroDex": ["firodex", "firo dex"],
    "GleecDex": ["gleecdex"],
    "Komodo Wallet": ["komodo wallet"],
    "Legacy Desktop": ["atomicdex desktop"],
    "Legacy Desktop CE": ["atomicdex desktop ce"],
    "LoreDex": ["lore dex"],
    "MM2 CLI": ["mm2cli"],
    "MM2GUI": ["mm2gui"],
    "NatureDEX": ["naturedex"],
    "Other": [],
    "PirateDex": ["piratedex"],
    "PytomicDex": ["pytomicdex", "pymakerbot"],
    "QA Tools": ["history_spammer_tool", "wasmtest", "artemii_dev", "qa_cli"],
    "ShibaDex": ["shibadex"],
    "SmartDEX": ["smartdex"],
    "SqueexeDEX": ["squeexedex", "squeexe wallet"],
    "SwapCase Desktop": ["swapcase desktop"],
    "Tokel IDO": ["tokel ido"],
    "Unknown": ["unknown"],
    "WebDEX": ["web_dex", "webdex"],
    "mmtools": ["mmtools"],
    "mpm": ["mpm"],
    "NN Seed": ["nn_seed"],
    "No Gui": ["nogui"],
}


DeFiVersions = {}


DeFiDevices = {
    "ios": ["iOS"],
    "web": ["Web"],
    "android": ["Android"],
    "darwin": ["Mac"],
    "linux": ["Linux"],
    "windows": ["Windows"],
}


def sum_num_str(val1, val2):
    x = Decimal(val1) + Decimal(val2)
    return format_10f(x)


def base_quote_from_pair(variant):
    split_variant = variant.split("_")
    try:
        if len(split_variant) == 4 and "OLD" in split_variant:
            if split_variant[1] == "OLD":
                base = f"{split_variant[0]}_{split_variant[1]}"
            if split_variant[3] == "OLD":
                quote = f"{split_variant[2]}_{split_variant[3]}"
        elif len(split_variant) == 3 and "OLD" in split_variant:
            if split_variant[2] == "OLD":
                base = split_variant[0]
                quote = f"{split_variant[1]}_{split_variant[2]}"
            elif split_variant[1] == "OLD":
                base = f"{split_variant[0]}_{split_variant[1]}"
                quote = split_variant[2]
        elif len(split_variant) == 2:
            base = split_variant[0]
            quote = split_variant[1]
        return base, quote
    except Exception as e:  # pragma: no cover
        return default.error(e)


def generic_pairs_time_filter(pairs_data, start_time, end_time):
    # TODO: handle first/last within variants
    last_traded = memcache.get_last_traded()
    last_traded = [
        i for i in last_traded if last_traded[i]["last_swap_time"] > start_time
    ]
    last_traded = [
        i for i in last_traded if last_traded[i]["last_swap_time"] < end_time
    ]
    return last_traded

    pass
    # pairs = sorted(list(set([transform.strip_pair_platforms(i) for i in data])))


def generic_pairs_deplatform(pairs_data):
    # TODO: handle first/last within variants
    last_traded = memcache.get_last_traded()

    pass
    # pairs = sorted(list(set([transform.strip_pair_platforms(i) for i in data])))
