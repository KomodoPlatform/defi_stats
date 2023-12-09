from decimal import Decimal
from logger import logger


def validate_ticker_id(ticker_id, valid_tickers):
    if ticker_id not in valid_tickers:
        msg = f"ticker_id '{ticker_id}' not in available pairs."
        msg += " Check the /api/v3/gecko/pairs endpoint for valid values."
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
