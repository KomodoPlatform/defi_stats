class MarketsDesc:
    def __init__(self) -> None:
        pass

    @property
    def adexio(self):
        return "Returns atomic swap counts over a variety of periods"

    @property
    def liquidity(self):
        desc = "Global liquidity on the orderbook for all pairs traded in the last 30 days."
        desc += (
            " There may be additional liquidity from pairs traded prior to this time."
        )
        return desc

    @property
    def orderbook(self):
        desc = "Current orderbook for a given pair, or all variants of a pair (including tokens)."
        desc += " Use `KMD-BEP20_LTC` format."
        return desc

    @property
    def pairs_last_trade(self):
        desc = "Returns last trade info and 24hr volume for all pairs traded in last 30 days"
        return desc

    @property
    def swaps24(self):
        desc = "Total swaps involving a specific coin (e.g. `KMD`, `KMD-BEP20`) in the last 24hrs."
        return desc

    @property
    def ticker_for_ticker(self):
        desc = (
            "Simple last price and liquidity for each market pair for a specific coin,"
        )
        desc += "e.g. `KMD`, `KMD-BEP20`."
        return desc

    @property
    def tickers_summary(self):
        desc = (
            "Total swaps and volume involving for each active ticker in the last 24hrs."
        )
        return desc


class StatsXyzDesc:
    def __init__(self) -> None:
        pass

    @property
    def atomicdexio(self):
        return "Returns atomic swap counts over a variety of periods"

    @property
    def current_liquidity(self):
        return "Global liquidity on the orderbook for all pairs."

    @property
    def fiat_rates(self):
        return "Coin prices in USD (where available)"

    @property
    def orderbook(self):
        return "Get Orderbook for a market pair in `KMD_LTC` format."

    @property
    def summary(self):
        return "24-hour price & volume for each pair traded in last 30 days."

    @property
    def summary_for_ticker(self):
        r = "24h price & volume for market pairs with a specific coin (e.g. `KMD`)"
        r += " traded in last 30 days."
        return r

    @property
    def swaps24(self):
        return "Total swaps involving a specific coin (e.g. `KMD`) in the last 24hrs."

    @property
    def ticker(self):
        r = "Simple last price and liquidity for each pair  (e.g. `KMD_LTC`),"
        r += "  traded in last 7 days."
        return r

    @property
    def ticker_for_ticker(self):
        r = "Simple last price and liquidity for each market pair for"
        r += " a specific coin (e.g. `KMD`)."
        return r

    @property
    def tickers_summary(self):
        return (
            "Total swaps and volume involving for each active ticker in the last 24hrs."
        )

    @property
    def trades(self):
        return "Trades for the last 'x' days for a pair in `KMD_LTC` format."

    @property
    def usd_volume_24hr(self):
        return "Volume (in USD) traded in last 24hrs."

    @property
    def volumes_ticker(self):
        return (
            "Daily coin volume (e.g. `KMD, KMD-BEP20, KMD-ALL`) traded last 'x' days."
        )


markets_desc = MarketsDesc()

stats_xyz_desc = StatsXyzDesc()
