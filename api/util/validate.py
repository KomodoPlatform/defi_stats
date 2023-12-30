from decimal import Decimal
from util.logger import logger
from lib.cache_load import load_coins_config
import lib


def reverse_ticker(ticker_id):
    return "_".join(ticker_id.split("_")[::-1])


def validate_ticker_id(ticker_id, valid_tickers, allow_reverse=False, allow_fail=False):
    if allow_reverse:
        inverse_valid_tickers = [
            f'{i.split("_")[1]}_{i.split("_")[0]}' for i in valid_tickers
        ]
        if ticker_id in inverse_valid_tickers:
            return "reversed"

    if ticker_id in valid_tickers:
        return "standard"

    msg = f"ticker_id '{ticker_id}' not in available pairs."
    msg += " Check the /api/v3/gecko/pairs endpoint for valid values."
    if allow_fail:
        return "failed"
    raise ValueError(msg)


def validate_positive_numeric(value, name, is_int=False):
    try:
        if Decimal(value) < 0:
            raise ValueError(f"{name} can not be negative!")
        if is_int and Decimal(value) % 1 != 0:
            raise ValueError(f"{name} must be an integer!")
    except Exception as e:
        logger.warning(f"{type(e)} Error validating {name}: {e}")
        raise ValueError(f"{name} must be numeric!")
    return True


def validate_orderbook_pair(base, quote):
    try:
        logger.muted(f"Validating {base}/{quote}")
        coins_config = load_coins_config()
        err = None
        if base not in coins_config.keys():
            err = {"error": f"CoinConfigNotFound for {base}"}
        if quote not in coins_config.keys():
            err = {"error": f"CoinConfigNotFound for {quote}"}
        if base in [i.ticker for i in lib.COINS.wallet_only]:
            err = {"error": f"CoinWalletOnlyException for {base}"}
        if quote in [i.ticker for i in lib.COINS.wallet_only]:
            err = {"error": f"CoinWalletOnlyException for {quote}"}
        if err is not None:
            return False
        return True
    except Exception as e:  # pragma: no cover
        logger.warning(e)
        return False
