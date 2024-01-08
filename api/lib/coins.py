#!/usr/bin/env python3
from dataclasses import dataclass
from lib.coin import Coin
from lib.cache import load_coins_config, load_gecko_source, load_generic_last_traded
from util.logger import logger

@dataclass
class Coins:  # pragma: no cover
    def __init__(self, testing=False):
        coins_config = load_coins_config(testing=testing)
        gecko_source = load_gecko_source(testing=testing)
        logger.loop(f"Getting generic_last_traded source for Coins")
        last_traded_cache = load_generic_last_traded(testing=testing)
        self.coins = [
            Coin(
                coin=i,
                gecko_source=gecko_source,
                coins_config=coins_config,
                last_traded_cache=last_traded_cache,
            )
            for i in coins_config
        ]

    @property
    def with_price(self):
        return [i for i in self.coins if i.is_priced]

    @property
    def with_mcap(self):
        return [i for i in self.coins if i.mcap > 0]

    @property
    def with_segwit(self):
        return [i for i in self.coins if i.has_segwit]

    @property
    def tradable_only(self):
        return [i for i in self.coins if i.is_tradable]

    @property
    def valid_only(self):
        return [i for i in self.coins if i.is_valid]

    @property
    def wallet_only(self):
        return [i for i in self.coins if i.is_wallet_only]

    @property
    def testnet_only(self):
        return [i for i in self.coins if i.is_testnet]

    @property
    def by_type(self):
        for coin in self.coins:
            if coin.type not in self.by_type_data:
                self.by_type_data.update({coin.type: []})
            self.by_type_data[coin.type].append(coin)
        return self.by_type_data
