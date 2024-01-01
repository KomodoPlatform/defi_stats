#!/usr/bin/env python3
from fixtures import template


def test_volumes_and_prices():
    volumes_and_prices = template.volumes_and_prices("24h")
    for i in volumes_and_prices:
        assert volumes_and_prices[i] == 0
    keys = volumes_and_prices.keys()
    assert "highest_price_24h" in keys
    assert "lowest_price_24h" in keys
    assert "price_change_percent_24h" in keys
