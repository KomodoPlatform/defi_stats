from decimal import Decimal, InvalidOperation
from typing import Any, List, Dict

from db.schema import DefiSwap
from util.logger import logger, timed
import util.cron as cron
import util.defaults as default
import util.helper as helper
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
            base, quote = helper.base_quote_from_pair(pair_str)
            base_mc = 0
            quote_mc = 0
            if base in gecko_source:
                base_mc = Decimal(gecko_source[base]["usd_market_cap"])
            if quote in gecko_source:
                quote_mc = Decimal(gecko_source[quote]["usd_market_cap"])
            if quote_mc < base_mc:
                pair_str = invert_pair(pair_str)
            elif quote_mc == base_mc:
                pair_str = "_".join(sorted([base, quote]))
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
        "ticker_id": i["ticker_id"],
        "pool_id": i["ticker_id"],
        "variants": i["variants"],
        "base_currency": i["base_currency"],
        "target_currency": i["quote_currency"],
        "bid": format_10f(i["highest_bid"]),
        "ask": format_10f(i["lowest_ask"]),
        "high": format_10f(i["highest_price_24hr"]),
        "low": format_10f(i["lowest_price_24hr"]),
        "base_volume": format_10f(i["base_volume"]),
        "target_volume": format_10f(i["quote_volume"]),
        "last_price": format_10f(i["last_swap_price"]),
        "last_trade": int(Decimal(i["last_swap_time"])),
        "last_swap_uuid": i["last_swap_uuid"],
        "trades_24hr": int(Decimal(i["trades_24hr"])),
        "volume_usd_24hr": format_10f(i["combined_volume_usd"]),
        "liquidity_in_usd": format_10f(i["liquidity_in_usd"]),
    }
    return data


@timed
def ticker_to_statsapi_summary(i):
    try:
        suffix = [k for k in i.keys() if k.startswith("trades_")][0].replace(
            "trades_", ""
        )
        if suffix == "24hr":
            alt_suffix = "24h"
        else:
            alt_suffix = suffix
        data = {
            "trading_pair": i["ticker_id"],
            "pair_swaps_count": int(Decimal(i[f"trades_{suffix}"])),
            "pair_liquidity_usd": Decimal(i["liquidity_in_usd"]),
            "pair_trade_value_usd": Decimal(i[f"combined_volume_usd"]),
            "base_currency": i["base_currency"],
            "base_volume": Decimal(i["base_volume"]),
            "base_price_usd": Decimal(i["base_usd_price"]),
            "base_trade_value_usd": Decimal(i["base_volume_usd"]),
            "base_liquidity_coins": Decimal(i["base_liquidity_coins"]),
            "base_liquidity_usd": Decimal(i["base_liquidity_usd"]),
            "quote_currency": i["quote_currency"],
            "quote_volume": Decimal(i["quote_volume"]),
            "quote_price_usd": Decimal(i["quote_usd_price"]),
            "quote_trade_value_usd": Decimal(i["quote_volume_usd"]),
            "quote_liquidity_coins": Decimal(i["quote_liquidity_coins"]),
            "quote_liquidity_usd": Decimal(i["quote_liquidity_usd"]),
            "newest_price": i["newest_price"],
            "oldest_price": i["oldest_price"],
            "newest_price_time": i["newest_price_time"],
            "oldest_price_time": i["oldest_price_time"],
            "highest_bid": Decimal(i["highest_bid"]),
            "lowest_ask": Decimal(i["lowest_ask"]),
            f"volume_usd_{alt_suffix}": Decimal(i[f"combined_volume_usd"]),
            f"highest_price_{alt_suffix}": Decimal(i[f"highest_price_{suffix}"]),
            f"lowest_price_{alt_suffix}": Decimal(i[f"lowest_price_{suffix}"]),
            f"price_change_{alt_suffix}": Decimal(i[f"price_change_{suffix}"]),
            f"price_change_pct_{alt_suffix}": Decimal(i[f"price_change_pct_{suffix}"]),
            "last_price": i["last_swap_price"],
            "last_trade": int(Decimal(i["last_swap_time"])),
            "last_swap_uuid": i["last_swap_uuid"],
            "variants": i["variants"],
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
    base, quote = helper.base_quote_from_pair(pair)
    return f"{strip_coin_platform(base)}_{strip_coin_platform(quote)}"


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


def invert_pair(pair_str):
    base, quote = helper.base_quote_from_pair(pair_str, True)
    return f"{base}_{quote}"



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




def last_trade_time_filter(last_traded, start_time, end_time):
    # TODO: handle first/last within variants
    last_traded = memcache.get_last_traded()
    last_traded = [
        i for i in last_traded if last_traded[i]["last_swap_time"] > start_time
    ]
    last_traded = [
        i for i in last_traded if last_traded[i]["last_swap_time"] < end_time
    ]
    return last_traded


def last_trade_deplatform(last_traded):
    data = {}
    for i in last_traded:
        pair = strip_coin_platform(i)
        if pair not in data:
            data.update({pair: last_traded[i]})
        else:
            if last_traded[i]["last_swap_time"] > data[pair]["last_swap_time"]:
                data[pair]["last_swap_time"] = last_traded[i]["last_swap_time"]
                data[pair]["last_swap_uuid"] = last_traded[i]["last_swap_uuid"]
                data[pair]["last_swap_price"] = last_traded[i]["last_swap_price"]

            if last_traded[i]["first_swap_time"] < data[pair]["first_swap_time"]:
                data[pair]["first_swap_time"] = last_traded[i]["first_swap_time"]
                data[pair]["first_swap_uuid"] = last_traded[i]["first_swap_uuid"]
                data[pair]["first_swap_price"] = last_traded[i]["first_swap_price"]
    return data


def tickers_deplatform(tickers_data):
    tickers = {}
    # Combine to pair without platforms
    for i in tickers_data["data"]:
        root_pair = strip_pair_platforms(i["ticker_id"])
        i["ticker_id"] = root_pair
        i["base_currency"] = strip_coin_platform(i["base_currency"])
        i["quote_currency"] = strip_coin_platform(i["quote_currency"])
        if root_pair not in tickers:
            i["trades_24hr"] = int(i["trades_24hr"])
            tickers.update({root_pair: i})
        else:
            if root_pair == "KMD_LTC": 
                logger.calc(i)
            j = tickers[root_pair]
            j["variants"] += i["variants"]
            j["trades_24hr"] += int(i["trades_24hr"])
            for key in [
                "combined_volume_usd",
                "liquidity_in_usd",
                "base_volume",
                "base_volume_usd",
                "base_liquidity_coins",
                "base_liquidity_usd",
                "quote_volume",
                "quote_volume_usd",
                "quote_liquidity_coins",
                "quote_liquidity_usd",
            ]:
                # Add to cumulative sum
                j[key] = sum_num_str(i[key], j[key])
            if Decimal(i["last_swap_time"]) > Decimal(j["last_swap_time"]):
                j["last_swap_price"] = i["last_swap_price"]
                j["last_swap_time"] = i["last_swap_time"]
                j["last_swap_uuid"] = i["last_swap_uuid"]

            if Decimal(i["first_swap_time"]) < Decimal(j["first_swap_time"]) or j["first_swap_time"] == 0:
                j["first_swap_price"] = i["first_swap_price"]
                j["first_swap_time"] = i["first_swap_time"]
                j["first_swap_uuid"] = i["first_swap_uuid"]

            if int(Decimal(i["newest_price_time"])) > int(Decimal(j["newest_price_time"])):
                j["newest_price_time"] = i["newest_price_time"]
                j["newest_price"] = i["newest_price"]

            if root_pair == "KMD_LTC": 
                logger.merge(i["oldest_price_time"])
                logger.merge(j["oldest_price_time"])
            if i["oldest_price_time"] < j["oldest_price_time"] or j["oldest_price_time"] == 0:
                j["oldest_price_time"] = i["oldest_price_time"]
                j["oldest_price"] = i["oldest_price"]

            if root_pair == "KMD_LTC": 
                logger.calc(i["oldest_price_time"])
                logger.calc(j["oldest_price_time"])
            if Decimal(i["highest_bid"]) > Decimal(j["highest_bid"]):
                j["highest_bid"] = i["highest_bid"]

            if Decimal(i["lowest_ask"]) < Decimal(j["lowest_ask"]):
                j["lowest_ask"] = i["lowest_ask"]

            if Decimal(i["highest_price_24hr"]) > Decimal(j["highest_price_24hr"]):
                j["highest_price_24hr"] = i["highest_price_24hr"]

            if Decimal(i["lowest_price_24hr"]) < Decimal(j["lowest_price_24hr"]) or j["lowest_price_24hr"] == 0:
                j["lowest_price_24hr"] = i["lowest_price_24hr"]

            j["price_change_24hr"] = format_10f(
                Decimal(j["newest_price"]) - Decimal(j["oldest_price"])
            )
            if Decimal(j["oldest_price"]) > 0:
                j["price_change_pct_24hr"] = format_10f(
                    Decimal(j["newest_price"]) / Decimal(j["oldest_price"]) - 1
                )
            else:
                j["price_change_pct_24hr"] = format_10f(0)
            if root_pair == "KMD_LTC": 
                logger.merge(j)
    tickers_data["data"] = tickers
    return tickers_data

def merge_segwit_swaps(variants, swaps):
    resp = []
    for i in variants:
        resp = resp + swaps[i]
    return sort_dict_list(resp, "finished_at", reverse=True)



@timed
def normalise_swap_data(data, is_success=None):
    try:
        gecko_source = memcache.get_gecko_source()
        for i in data:
            pair_raw = f'{i["maker_coin"]}_{i["taker_coin"]}'
            pair = order_pair_by_market_cap(pair_raw)
            pair_reverse = invert_pair(pair)
            pair_std = strip_pair_platforms(pair)
            pair_std_reverse = invert_pair(pair_std)
            if pair_raw == pair:
                trade_type = "buy"
                price = Decimal(i["maker_amount"] / i["taker_amount"])
                reverse_price = Decimal(i["taker_amount"] / i["maker_amount"])
            else:
                trade_type = "sell"
                price = Decimal(i["taker_amount"] / i["maker_amount"])
                reverse_price = Decimal(i["maker_amount"] / i["taker_amount"])
            i.update(
                {
                    "pair": pair,
                    "pair_std": pair_std,
                    "pair_reverse": pair_reverse,
                    "pair_std_reverse": pair_std_reverse,
                    "trade_type": trade_type,
                    "maker_coin_ticker": strip_coin_platform(i["maker_coin"]),
                    "maker_coin_platform": get_coin_platform(i["maker_coin"]),
                    "taker_coin_ticker": strip_coin_platform(i["taker_coin"]),
                    "taker_coin_platform": get_coin_platform(i["taker_coin"]),
                    "price": price,
                    "reverse_price": reverse_price,
                }
            )
            if "is_success" not in i:
                if is_success is not None:
                    if is_success:
                        i.update({"is_success": 1})
                    else:
                        i.update({"is_success": 0})
                else:
                    i.update({"is_success": -1})

            for k, v in i.items():
                if k in [
                    "maker_pubkey",
                    "taker_pubkey",
                    "taker_gui",
                    "maker_gui",
                    "taker_version",
                    "maker_version",
                ]:
                    if v in [None, ""]:
                        i.update({k: ""})
                if k in [
                    "maker_coin_usd_price",
                    "taker_coin_usd_price",
                    "started_at",
                    "finished_at",
                ]:
                    if v in [None, ""]:
                        i.update({k: 0})
    except Exception as e:
        return default.error(e)
    msg = "Data normalised"
    return default.result(msg=msg, data=data, loglevel="updated")


@timed
def cipi_to_defi_swap(cipi_data, defi_data=None):
    """
    Compares with existing to select best value where there is a conflict,
    or returns normalised data from a source database with derived fields
    calculated or defaults applied
    """
    try:
        if defi_data is None:
            for i in ["taker_gui", "maker_gui", "taker_version", "maker_version"]:
                if i not in cipi_data:
                    cipi_data.update({i: ""})
            data = DefiSwap(
                uuid=cipi_data["uuid"],
                taker_amount=cipi_data["taker_amount"],
                taker_coin=cipi_data["taker_coin"],
                taker_gui=cipi_data["taker_gui"],
                taker_pubkey=cipi_data["taker_pubkey"],
                taker_version=cipi_data["taker_version"],
                maker_amount=cipi_data["maker_amount"],
                maker_coin=cipi_data["maker_coin"],
                maker_gui=cipi_data["maker_gui"],
                maker_pubkey=cipi_data["maker_pubkey"],
                maker_version=cipi_data["maker_version"],
                started_at=int(cipi_data["started_at"].timestamp()),
                # Not in Cipi's DB, but better than zero.
                finished_at=cipi_data["started_at"],
                # Not in Cipi's DB, but able to derive.
                price=cipi_data["price"],
                reverse_price=cipi_data["reverse_price"],
                is_success=cipi_data["is_success"],
                maker_coin_platform=cipi_data["maker_coin_platform"],
                maker_coin_ticker=cipi_data["maker_coin_ticker"],
                taker_coin_platform=cipi_data["taker_coin_platform"],
                taker_coin_ticker=cipi_data["taker_coin_ticker"],
                # Extra columns
                trade_type=cipi_data["trade_type"],
                pair=cipi_data["pair"],
                pair_reverse=invert_pair(cipi_data["pair"]),
                pair_std=strip_pair_platforms(cipi_data["pair"]),
                pair_std_reverse=strip_pair_platforms(
                    invert_pair(cipi_data["pair"])
                ),
                last_updated=int(cron.now_utc()),
            )
        else:
            for i in [
                "taker_coin",
                "maker_coin",
                "taker_gui",
                "maker_gui",
                "taker_pubkey",
                "maker_pubkey",
                "taker_version",
                "maker_version",
                "taker_coin_ticker",
                "taker_coin_ticker",
                "taker_coin_platform",
                "taker_coin_platform",
            ]:
                if cipi_data[i] != defi_data[i]:
                    if cipi_data[i] in ["", "None", "unknown", None]:
                        cipi_data[i] = defi_data[i]
                    elif defi_data[i] in ["", "None", "unknown", None]:
                        defi_data[i] = cipi_data[i]
                    elif isinstance(defi_data[i], str):
                        if len(defi_data[i]) == 0:
                            defi_data[i] = cipi_data[i]
                        pass
                    else:
                        # This shouldnt happen
                        logger.warning("Mismatch on incoming cipi data vs defi data:")
                        logger.warning(f"{cipi_data[i]} vs {defi_data[i]}")

            data = DefiSwap(
                uuid=cipi_data["uuid"],
                taker_coin=cipi_data["taker_coin"],
                taker_gui=cipi_data["taker_gui"],
                taker_pubkey=cipi_data["taker_pubkey"],
                taker_version=cipi_data["taker_version"],
                maker_coin=cipi_data["maker_coin"],
                maker_gui=cipi_data["maker_gui"],
                maker_pubkey=cipi_data["maker_pubkey"],
                maker_version=cipi_data["maker_version"],
                maker_coin_platform=cipi_data["maker_coin_platform"],
                maker_coin_ticker=cipi_data["maker_coin_ticker"],
                taker_coin_platform=cipi_data["taker_coin_platform"],
                taker_coin_ticker=cipi_data["taker_coin_ticker"],
                taker_amount=max(cipi_data["taker_amount"], defi_data["taker_amount"]),
                maker_amount=max(cipi_data["maker_amount"], defi_data["maker_amount"]),
                started_at=max(
                    int(cipi_data["started_at"].timestamp()), defi_data["started_at"]
                ),
                finished_at=max(
                    int(cipi_data["started_at"].timestamp()), defi_data["finished_at"]
                ),
                is_success=max(cipi_data["is_success"], defi_data["is_success"]),
                # Not in Cipi's DB, but derived from taker/maker amounts.
                price=max(cipi_data["price"], defi_data["price"]),
                reverse_price=max(
                    cipi_data["reverse_price"], defi_data["reverse_price"]
                ),
                # Not in Cipi's DB
                maker_coin_usd_price=max(0, defi_data["maker_coin_usd_price"]),
                taker_coin_usd_price=max(0, defi_data["taker_coin_usd_price"]),
                # Extra columns
                trade_type=defi_data["trade_type"],
                pair=defi_data["pair"],
                pair_reverse=invert_pair(cipi_data["pair"]),
                pair_std=strip_pair_platforms(cipi_data["pair"]),
                pair_std_reverse=strip_pair_platforms(
                    invert_pair(cipi_data["pair"])
                ),
                last_updated=int(cron.now_utc()),
            )
        if isinstance(data.finished_at, int) and isinstance(data.started_at, int):
            data.duration = data.finished_at - data.started_at
        else:
            data.duration = -1
    except Exception as e:
        return default.error(e)
    msg = "cipi to defi conversion complete"
    return default.result(msg=msg, data=data, loglevel="muted")


@timed
def mm2_to_defi_swap(mm2_data, defi_data=None):
    """
    Compares with existing to select best value where there is a conflict
    """
    try:
        if defi_data is None:
            for i in ["taker_gui", "maker_gui", "taker_version", "maker_version"]:
                if i not in mm2_data:
                    mm2_data.update({i: ""})
            data = DefiSwap(
                uuid=mm2_data["uuid"],
                taker_amount=mm2_data["taker_amount"],
                taker_coin=mm2_data["taker_coin"],
                taker_pubkey=mm2_data["taker_pubkey"],
                maker_amount=mm2_data["maker_amount"],
                maker_coin=mm2_data["maker_coin"],
                maker_pubkey=mm2_data["maker_pubkey"],
                is_success=mm2_data["is_success"],
                maker_coin_platform=mm2_data["maker_coin_platform"],
                maker_coin_ticker=mm2_data["maker_coin_ticker"],
                taker_coin_platform=mm2_data["taker_coin_platform"],
                taker_coin_ticker=mm2_data["taker_coin_ticker"],
                started_at=mm2_data["started_at"],
                finished_at=mm2_data["finished_at"],
                # Not in MM2 DB, but able to derive.
                price=mm2_data["price"],
                reverse_price=mm2_data["reverse_price"],
                # Not in MM2 DB, using default.
                taker_gui=mm2_data["taker_gui"],
                taker_version=mm2_data["taker_version"],
                maker_gui=mm2_data["maker_gui"],
                maker_version=mm2_data["maker_version"],
                # Extra columns
                trade_type=mm2_data["trade_type"],
                pair=mm2_data["pair"],
                pair_reverse=invert_pair(mm2_data["pair"]),
                pair_std=strip_pair_platforms(mm2_data["pair"]),
                pair_std_reverse=strip_pair_platforms(
                    invert_pair(mm2_data["pair"])
                ),
                last_updated=int(cron.now_utc()),
            )
        else:
            for i in [
                "taker_coin",
                "maker_coin",
                "taker_pubkey",
                "maker_pubkey",
                "taker_coin_ticker",
                "taker_coin_ticker",
                "taker_coin_platform",
                "taker_coin_platform",
            ]:
                if mm2_data[i] != defi_data[i]:
                    if mm2_data[i] in ["", "None", None, -1, "unknown"]:
                        mm2_data[i] = defi_data[i]
                    elif defi_data[i] in ["", "None", None, -1, "unknown"]:
                        pass
                    elif isinstance(mm2_data[i], str):
                        if len(mm2_data[i]) == 0:
                            mm2_data[i] = defi_data[i]
                    else:
                        # This shouldnt happen
                        logger.warning("Mismatch on incoming mm2 data vs defi data:")
                        logger.warning(f"{mm2_data[i]} vs {defi_data[i]}")
                        logger.warning(f"{type(mm2_data[i])} vs {type(defi_data[i])}")
            data = DefiSwap(
                uuid=mm2_data["uuid"],
                taker_coin=mm2_data["taker_coin"],
                taker_pubkey=mm2_data["taker_pubkey"],
                maker_coin=mm2_data["maker_coin"],
                maker_pubkey=mm2_data["maker_pubkey"],
                maker_coin_platform=mm2_data["maker_coin_platform"],
                maker_coin_ticker=mm2_data["maker_coin_ticker"],
                taker_coin_platform=mm2_data["taker_coin_platform"],
                taker_coin_ticker=mm2_data["taker_coin_ticker"],
                taker_amount=max(mm2_data["taker_amount"], defi_data["taker_amount"]),
                maker_amount=max(mm2_data["maker_amount"], defi_data["maker_amount"]),
                started_at=max(mm2_data["started_at"], defi_data["started_at"]),
                finished_at=max(mm2_data["finished_at"], defi_data["finished_at"]),
                is_success=max(mm2_data["is_success"], defi_data["is_success"]),
                maker_coin_usd_price=max(0, defi_data["maker_coin_usd_price"]),
                taker_coin_usd_price=max(0, defi_data["taker_coin_usd_price"]),
                # Not in MM2 DB, but derived from taker/maker amounts.
                price=max(mm2_data["price"], defi_data["price"]),
                reverse_price=max(
                    mm2_data["reverse_price"], defi_data["reverse_price"]
                ),
                # Not in MM2 DB, keep existing value
                taker_gui=defi_data["taker_gui"],
                maker_gui=defi_data["maker_gui"],
                taker_version=defi_data["taker_version"],
                maker_version=defi_data["maker_version"],
                # Extra columns
                trade_type=defi_data["trade_type"],
                pair=defi_data["pair"],
                pair_reverse=invert_pair(defi_data["pair"]),
                pair_std=strip_pair_platforms(defi_data["pair"]),
                pair_std_reverse=strip_pair_platforms(
                    invert_pair(defi_data["pair"])
                ),
                last_updated=int(cron.now_utc()),
            )
        if isinstance(data.finished_at, int) and isinstance(data.started_at, int):
            data.duration = data.finished_at - data.started_at
        else:
            data.duration = -1

    except Exception as e:
        return default.error(e)
    msg = "mm2 to defi conversion complete"
    return default.result(msg=msg, data=data, loglevel="muted")
