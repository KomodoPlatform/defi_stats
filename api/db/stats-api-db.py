
class SqliteDB:
    def __init__(self, db_path, dict_format=False, testing=False):
        self.utils = Utils()
        self.files = Files(testing)
        self.testing = testing
        self.conn = sqlite3.connect(db_path)
        if dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        self.gecko_data = self.utils.load_jsonfile(self.files.gecko_data)

    def close(self):
        self.conn.close()

    def get_pairs(self, days: int = 7) -> list:
        """
        Returns a list of pairs (as a list of tuples) with at least one
        successful swap in the last 'x' days.
        """
        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
        sql = f"SELECT DISTINCT maker_coin_ticker, taker_coin_ticker FROM stats_swaps \
                WHERE started_at > {timestamp} AND is_success=1;"
        self.sql_cursor.execute(sql)
        pairs = self.sql_cursor.fetchall()
        sorted_pairs = [tuple(sorted(pair)) for pair in pairs]
        pairs = list(set(sorted_pairs))
        adjusted = []
        for pair in pairs:
            if pair[0] in self.gecko_data:
                if pair[1] in self.gecko_data:
                    if (
                        self.gecko_data[pair[1]]["usd_market_cap"]
                        < self.gecko_data[pair[0]]["usd_market_cap"]
                    ):
                        pair = (pair[1], pair[0])
                else:
                    pair = (pair[1], pair[0])
            adjusted.append(pair)
        return adjusted

    def get_swaps_for_pair(self, pair: tuple, timestamp: int = -1) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        try:
            if timestamp == -1:
                timestamp = int((datetime.now() - timedelta(days=1)).strftime("%s"))
            t = (
                timestamp,
                pair[0],
                pair[1],
            )
            sql = "SELECT * FROM stats_swaps WHERE started_at > ? \
                    AND maker_coin_ticker=? \
                    AND taker_coin_ticker=? \
                    AND is_success=1;"
            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            self.sql_cursor.execute(
                sql,
                t,
            )
            data = self.sql_cursor.fetchall()
            swaps_for_pair_a_b = [dict(row) for row in data]

            for swap in swaps_for_pair_a_b:
                swap["trade_type"] = "buy"
            sql = "SELECT * FROM stats_swaps WHERE started_at > ? \
                    AND taker_coin_ticker=? \
                    AND maker_coin_ticker=? \
                    AND is_success=1;"
            self.sql_cursor.execute(
                sql,
                t,
            )
            data = self.sql_cursor.fetchall()
            swaps_for_pair_b_a = [dict(row) for row in data]
            for swap in swaps_for_pair_b_a:
                temp_maker_amount = swap["maker_amount"]
                swap["maker_amount"] = swap["taker_amount"]
                swap["taker_amount"] = temp_maker_amount
                swap["trade_type"] = "sell"
            swaps_for_pair = swaps_for_pair_a_b + swaps_for_pair_b_a
            return swaps_for_pair
        except Exception as e:
            logger.warning(f"{type(e)} Error in [get_swaps_for_pair]: {e}")
            return []

    def get_last_price_for_pair(self, base: str, quote: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()

        try:
            swap_price = None
            swap_time = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{base}' \
                    AND taker_coin_ticker='{quote}' AND is_success=1 \
                    ORDER BY started_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp = self.sql_cursor.fetchone()
            if resp is not None:
                swap_price = Decimal(resp["taker_amount"]) / Decimal(
                    resp["maker_amount"]
                )
                swap_time = resp["started_at"]
        except Exception as e:
            logger.warning(f"{type(e)} Error getting swap_price for {base}/{quote}: {e}")

        try:
            swap_price2 = None
            swap_time2 = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{quote}' \
                    AND taker_coin_ticker='{base}' AND is_success=1 \
                    ORDER BY started_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp2 = self.sql_cursor.fetchone()
            if resp2 is not None:
                swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                    resp2["taker_amount"]
                )
                swap_time2 = resp2["started_at"]
        except Exception as e:
            logger.warning(f"{type(e)} Error getting swap_price2 for {base}/{quote}: {e}")

        if swap_price and swap_price2:
            if swap_time > swap_time2:
                price = swap_price
            else:
                price = swap_price2
        elif swap_price:
            price = swap_price
        elif swap_price2:
            price = swap_price2
        else:
            price = 0
        return price

