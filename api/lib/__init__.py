from lib.coins import Coins
from util import templates as template
from lib.pair import get_all_coin_pairs

print("Init coins...")
COINS = Coins()
PRICED_COINS = [i.coin for i in COINS.with_price]
KMD_PAIRS = get_all_coin_pairs("KMD", PRICED_COINS)
KMD_PAIRS_INFO = [
    template.pair_info(i)
    for i in KMD_PAIRS
    if i.split("_")[0] in PRICED_COINS
    and i.split("_")[1] in PRICED_COINS
]
