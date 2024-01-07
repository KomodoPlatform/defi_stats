from decimal import Decimal
from lib.cache_item import CacheItem
from util.logger import logger
from util.defaults import default_error


def load_gecko_source(testing=False):
    try:
        # logger.merge("Loading Gecko source")
        return CacheItem("gecko_source", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_gecko_source]: {e}")
        return {}


def load_coins_config(testing=False):
    try:
        return CacheItem("coins_config", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_coins_config]: {e}")
        return {}


def load_coins(testing=False):  # pragma: no cover
    try:
        return CacheItem("coins", testing=testing).data
    except Exception as e:
        logger.error(f"{type(e)} Error in [load_coins]: {e}")
        return {}


def load_generic_last_traded(testing=False):
    try:
        return CacheItem("generic_last_traded", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_generic_last_traded]: {e}")
        return {}


def load_generic_pairs(testing=False):
    try:
        return CacheItem("generic_pairs", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_coins_config]: {e}")
        return {}


def get_gecko_price_and_mcap(ticker, gecko_source=None, testing=False) -> float:
    try:
        if gecko_source is None:
            # logger.merge("Loading Gecko get_gecko_price_and_mcap")
            gecko_source = load_gecko_source()
        if ticker in gecko_source:
            price = Decimal(gecko_source[ticker]["usd_price"])
            mcap = Decimal(gecko_source[ticker]["usd_market_cap"])
            return price, mcap
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0), Decimal(0)  # pragma: no cover


def get_segwit_coins(coins_config, testing=False) -> list:
    try:
        segwit_coins = [i for i in coins_config if "-segwit" in i]
        segwit_coins += [i.split("-")[0] for i in coins_config if "-segwit" in i]
        return segwit_coins
    except Exception as e:  # pragma: no cover
        return default_error(e)
