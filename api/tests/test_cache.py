import util.memcache as memcache
from lib.cache import Cache


def test_cache():

    cache = Cache()

    for i in [
        "generic_adex_fortnite",
        "generic_last_traded",
        "generic_summary",
        "generic_tickers",
    ]:
        cache_item = cache.get_item(i)
        data = cache_item.save()
        assert "error" not in data
