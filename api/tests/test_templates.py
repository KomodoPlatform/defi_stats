#!/usr/bin/env python3
from fixtures import setup_templates


def test_gecko_info(setup_templates):
    templates = setup_templates
    r = templates.gecko_info("komodo")
    assert r["usd_market_cap"] == 0
    assert r["usd_price"] == 0
    assert r["coingecko_id"] == "komodo"


def test_volumes_and_prices(setup_templates):
    templates = setup_templates
    volumes_and_prices = templates.volumes_and_prices("24h")
    for i in volumes_and_prices:
        assert volumes_and_prices[i] == 0
    keys = volumes_and_prices.keys()
    assert "highest_price_24h" in keys
    assert "lowest_price_24h" in keys
    assert "price_change_percent_24h" in keys
