from lib.cache import Cache
from util.logger import logger


def test_cache():
    cache = Cache()

    for i in [
        "coin_volumes_24hr",
        "pairs_last_traded",
        "pair_prices_24hr",
        "pair_volumes_24hr",
        "pair_volumes_14d",
        "pair_volumes_alltime",
        "gecko_pairs",
        "pairs_orderbook_extended",
        "markets_summary",
        "stats_api_summary",
        "adex_24hr",
        "adex_fortnite",
        "adex_alltime",
        "prices_tickers_v1",
        "prices_tickers_v2",
        "tickers",
    ]:
        cache_item = cache.get_item(i)
        data = cache_item.save()
        logger.calc(f"Testing {i}")
        assert "error" not in data
