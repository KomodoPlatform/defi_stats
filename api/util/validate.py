from decimal import Decimal
from util.logger import logger
from util.exceptions import DataStructureError, BadPairFormatError


def is_valid_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def ticker_id(ticker_id, valid_tickers, allow_reverse=False, allow_fail=False):
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


def loop_data(data, cache_item, netid=None):
    try:
        if data is None:
            return False
        if "error" in data:
            raise DataStructureError(
                f"Unexpected data structure returned for {cache_item.name} ({netid})"
            )
        if len(data) > 0:
            return True
        else:
            msg = (
                f"{cache_item.name} not updated because input data was empty ({netid})"
            )
            logger.warning(msg)
            return False
    except Exception as e:
        msg = f"{cache_item.name} not updated because invalid: {e} ({netid})"
        logger.warning(msg)
        return False


def orderbook_pair(base, quote, coins_config):
    try:
        logger.muted(f"Validating {base}/{quote}")
        err = None
        # TODO: Create alternative which does not risk circular import
        # wallet_only = [i.ticker for i in lib.COINS.wallet_only]
        # with_segwit = [i.ticker for i in lib.COINS.with_segwit]
        if base.replace("-segwit", "") == quote.replace("-segwit", ""):
            err = {"error": f"BaseQuoteSameError for {base}"}
        if base not in coins_config.keys():
            err = {"error": f"CoinConfigNotFound for {base}"}
        if quote not in coins_config.keys():
            err = {"error": f"CoinConfigNotFound for {quote}"}
        '''
        if base in wallet_only:
            if base in with_segwit:
                if f"{base}-segwit" in wallet_only:
                    err = {"error": f"CoinWalletOnlyException {base}"}
            else:
                err = {"error": f"CoinWalletOnlyException for {base}"}
        if quote in wallet_only:
            if quote in with_segwit:
                if f"{quote}-segwit" in wallet_only:
                    err = {"error": f"CoinWalletOnlyException {quote}"}
            else:
                err = {"error": f"CoinWalletOnlyException for {quote}"}
        '''
        if err is not None:
            return False
        return True
    except Exception as e:  # pragma: no cover
        logger.warning(e)
        return False


def json_obj(data, outer=True):
    if outer:
        try:
            if isinstance(data, list):
                data = data[0]
            data.keys()
        except Exception as e:
            logger.error(e)
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
