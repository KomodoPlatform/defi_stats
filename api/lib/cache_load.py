from decimal import Decimal
from lib.cache_item import CacheItem
from util.logger import logger
from util.defaults import default_error


def load_gecko_source(testing=False):
    try:
        return CacheItem("gecko_source", testing=False).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_gecko_source]: {e}")
        return {}


def get_gecko_price_and_mcap(ticker, testing=False) -> float:
    try:
        gecko_source = CacheItem("gecko_source", testing=testing).data
        if ticker in gecko_source:
            price = Decimal(gecko_source[ticker]["usd_price"])
            mcap = Decimal(gecko_source[ticker]["usd_market_cap"])
            return price, mcap
    except KeyError as e:  # pragma: no cover
        logger.info(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.info(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0), Decimal(0)  # pragma: no cover


def load_coins_config(testing=False):
    try:
        return CacheItem("coins_config", testing=False).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_coins_config]: {e}")
        return {}


def get_segwit_coins(testing=False) -> list:
    try:
        coins_config_cache = load_coins_config(testing=False)
        segwit_coins = [i for i in coins_config_cache if "-segwit" in i]
        segwit_coins += [i.split("-")[0] for i in coins_config_cache if "-segwit" in i]
        return segwit_coins
    except Exception as e:  # pragma: no cover
        return default_error(e)


def load_coins(testing=False):  # pragma: no cover
    try:
        return CacheItem("coins", testing=False).data
    except Exception as e:
        logger.error(f"{type(e)} Error in [load_coins]: {e}")
        return {}