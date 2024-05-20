from decimal import Decimal
from util.logger import logger, timed
from util.exceptions import DataStructureError, BadPairFormatError
from util.transform import deplatform
from util.transform import derive, sortdata, invert
import util.defaults as default


def is_valid_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def positive_numeric(value, name, is_int=False):
    try:
        if Decimal(value) < 0:
            logger.warning(f"{name} can not be negative!")
            raise ValueError(f"{name} can not be negative!")
        if is_int and Decimal(value) % 1 != 0:
            logger.warning(f"{name} must be an integer!")
            raise ValueError(f"{name} must be an integer!")
    except Exception as e:
        logger.warning(f"{type(e)} Error validating {name}: {e}")
        raise ValueError(f"{name} must be numeric!")
    return True


@timed
def orderbook_request(base, quote, coins_config):
    try:
        if base not in coins_config:
            msg = f"dex_api.get_orderbook {base} not in coins_config!"
            raise ValueError
        elif quote not in coins_config:
            msg = f"dex_api.get_orderbook {quote} not in coins_config!"
            raise ValueError
        elif coins_config[base]["wallet_only"]:
            msg = f"dex_api.get_orderbook {base} is wallet only!"
            raise ValueError
        elif coins_config[quote]["wallet_only"]:
            msg = f"dex_api.get_orderbook {quote} is wallet only!"
            raise ValueError
        elif base == quote:
            msg = f"dex_api.get_orderbook {quote} == {quote}!"
            raise ValueError
    except Exception as e:
        default.result(msg=f"{e} {msg}", data=False, loglevel="warning", ignore_until=0)
        return False
    return True


def loop_data(data, cache_item):
    try:
        if data is None:
            return False
        if "error" in data:
            raise DataStructureError(
                f"Unexpected data structure returned for {cache_item.name}"
            )
        if len(data) > 0:
            return True
        else:
            msg = f"{cache_item.name} not updated because input data was empty"
            logger.warning(msg)
            return False
    except Exception as e:
        msg = f"{cache_item.name} not updated because invalid: {e}"
        logger.warning(msg)
        return False


def is_bridge_swap(pair_str):
    root_pairing = deplatform.pair(pair_str)
    if len(set(root_pairing.split("_"))) == 1:
        return True
    return False


def is_bridge_swap_duplicate(pair_str, gecko_source):
    if is_bridge_swap(pair_str) is False:
        return False
    if pair_str != sortdata.pair_by_market_cap(pair_str, gecko_source=gecko_source):
        return True
    return False


def json_obj(data, outer=True):
    if outer:
        try:
            if isinstance(data, list):
                data = data[0]
            data.keys()
        except Exception as e:
            if isinstance(data, str | int | float | Decimal):
                return False
            logger.error(e)
            logger.error(data)
            return False
    # Recursivety checks nested data
    if isinstance(data, dict):
        return all(json_obj(value, False) for value in data.values())
    elif isinstance(data, list):
        return all(json_obj(item, False) for item in data)
    elif isinstance(data, (int, float, str, bool, type(None))):
        # We can add custom validation here, for example if an error
        # message ends up in the json data which should not be there
        return True
    else:
        logger.warning(f"{data} Failed json validation {type(data)}")
        return False


def pair(pair_str):
    if not isinstance(pair_str, str):
        raise TypeError
    if "_" not in pair_str:
        raise BadPairFormatError(msg="Pair must be in format 'KMD_LTC'!")
    return True


def is_source_db(db_file) -> bool:
    if db_file.endswith("MM2.db"):
        return True
    return False


def is_7777(db_file) -> bool:
    if db_file.startswith("seed"):
        return True
    return False


def is_pair_priced(pair_str, gecko_source):
    base, quote = derive.base_quote(pair_str)
    if base.replace("-segwit", "") in gecko_source:
        if quote.replace("-segwit", "") in gecko_source:
            x = gecko_source[base.replace("-segwit", "")]["usd_price"]
            y = gecko_source[quote.replace("-segwit", "")]["usd_price"]
            if x > 0 and y > 0:  # pragma: no cover
                return True
    return False


def ensure_valid_pair(data, gecko_source):
    try:
        data["maker_coin_ticker"] = deplatform.coin(data["maker_coin"])
        data["maker_coin_platform"] = derive.coin_platform(data["maker_coin"])
        data["taker_coin_ticker"] = deplatform.coin(data["taker_coin"])
        data["taker_coin_platform"] = derive.coin_platform(data["taker_coin"])
        if data["taker_coin_platform"] != "":
            _base = f"{data['taker_coin_ticker']}-{data['taker_coin_platform']}"
        else:
            _base = f"{data['taker_coin_ticker']}"
        if data["maker_coin_platform"] != "":
            _quote = f"{data['maker_coin_ticker']}-{data['maker_coin_platform']}"
        else:
            _quote = f"{data['maker_coin_ticker']}"
        _pair = f"{_base}_{_quote}"
        data["pair"] = sortdata.pair_by_market_cap(_pair, gecko_source=gecko_source)
        data["pair_std"] = deplatform.pair(data["pair"])
        data["pair_reverse"] = invert.pair(data["pair"])
        data["pair_std_reverse"] = invert.pair(data["pair_std"])
        # Assign price and trade_type
        if deplatform.pair(_pair) == data["pair_std"]:
            trade_type = "sell"
            price = Decimal(data["maker_amount"]) / Decimal(data["taker_amount"])
            reverse_price = Decimal(data["taker_amount"]) / Decimal(
                data["maker_amount"]
            )
        elif deplatform.pair(_pair) == data["pair_std_reverse"]:
            trade_type = "buy"
            price = Decimal(data["taker_amount"]) / Decimal(data["maker_amount"])
            reverse_price = Decimal(data["maker_amount"]) / Decimal(
                data["taker_amount"]
            )
        data.update(
            {
                "trade_type": trade_type,
                "price": price,
                "reverse_price": reverse_price,
            }
        )
    except Exception as e:
        logger.warning(e)
    return data
