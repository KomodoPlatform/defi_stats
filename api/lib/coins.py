#!/usr/bin/env python3
from dataclasses import dataclass
import db
import lib


@dataclass
class Coins:  # pragma: no cover
    def __init__(self):
        coins_config = lib.load_coins_config()
        gecko_source = lib.load_gecko_source()
        last_traded_cache = lib.load_generic_last_traded()
        self.pg_query = db.SqlQuery(
            gecko_source=gecko_source, coins_config=coins_config
        )
        self.coins = [
            lib.Coin(
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
