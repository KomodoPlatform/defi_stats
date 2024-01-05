#!/usr/bin/env python3
from dataclasses import dataclass
from lib.coin import Coin
from lib.cache_load import load_coins_config


@dataclass
class Coins:  # pragma: no cover
    def __init__(self):
        self.coins = [Coin(coin=i) for i in load_coins_config()]

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
