import os
from lib.coins import Coins
from lib.coin import Coin, get_gecko_price_and_mcap
from util import templates as template
from lib.pair import get_all_coin_pairs
from lib.cache import (
    load_coins_config,
    load_gecko_source,
    CacheItem,
    Cache,
    load_generic_last_traded,
    load_generic_pairs,
    load_adex_fortnite,
    load_statsapi_summary,
)


print("Init coins...")
if "IS_TESTING" in os.environ:
    COINS = Coins(testing=True)
else:
    COINS = Coins()
PRICED_COINS = list(set([i.coin.replace("-segwit", "") for i in COINS.with_price]))
KMD_PAIRS = get_all_coin_pairs("KMD", PRICED_COINS)
KMD_PAIRS.sort()
KMD_PAIRS_INFO = [
    i
    for i in KMD_PAIRS
    if i.split("_")[0] in PRICED_COINS and i.split("_")[1] in PRICED_COINS
]
