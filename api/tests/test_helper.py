import pytest
from decimal import Decimal
from fixtures import historical_data, historical_trades
from util.helper import list_json_key, sum_json_key, sum_json_key_10f


def test_list_json_key(historical_data, historical_trades):
    assert list_json_key(historical_trades, "type",
                         "buy") == historical_data["buy"]
    assert list_json_key(historical_trades, "type",
                         "sell") == historical_data["sell"]


def test_sum_json_key(historical_data):
    assert sum_json_key(
        historical_data["buy"], "target_volume") == Decimal("60")
    assert sum_json_key(
        historical_data["sell"], "target_volume") == Decimal("30")


def test_sum_json_key_10f(historical_data, historical_trades):
    assert sum_json_key_10f(
        historical_data["buy"], "target_volume") == "60.0000000000"
