#!/usr/bin/env python3
from decimal import Decimal
from fastapi import HTTPException


def format_10f(number: float) -> str:
    '''
    Format a float to 10 decimal places.
    '''
    return f"{number:.10f}"


def list_json_key(data: dict, key: str, filter_value: str) -> Decimal:
    '''
    list of key values from dicts.
    '''
    return [i for i in data if i[key] == filter_value]


def sum_json_key(data: dict, key: str) -> Decimal:
    '''
    Sum a key from a list of dicts.
    '''
    return sum(Decimal(d[key]) for d in data)


def sum_json_key_10f(data: dict, key: str) -> str:
    '''
    Sum a key from a list of dicts and format to 10 decimal places.
    '''
    return format_10f(sum_json_key(data, key))


def validate_ticker(ticker_id: str):
    if len(ticker_id) > 32:
        raise HTTPException(
            status_code=400,
            detail="Pair cant be longer than 32 symbols"
        )
    elif len(ticker_id.split("_")) != 2:
        raise HTTPException(
            status_code=400,
            detail="Pair should be in format BASE_TARGET"
        )
