#!/usr/bin/env python3
from datetime import timezone
from util.logger import logger
from db.sqldb import SqlUpdate


def get_ext_swaps(days_since=1):
    ext_cursor.execute(
        "SELECT * FROM swaps WHERE started_at >= now() - INTERVAL "
        + str(days_since)
        + " DAY ORDER BY started_at;"
    )
    result = ext_cursor.fetchall()
    for x in result:
        row = swaps_row()
        row.uuid = x[1]
        row.started_at = x[2]
        row.taker_coin = x[3]
        row.taker_amount = x[4]
        row.taker_gui = x[5]
        row.taker_version = x[6]
        row.taker_pubkey = x[7]
        row.maker_coin = x[8]
        row.maker_amount = x[9]
        row.maker_gui = x[10]
        row.maker_version = x[11]
        row.maker_pubkey = x[12]
        row.timestamp = int(x[2].replace(tzinfo=timezone.utc).timestamp())
        row.update()


def get_ext_swaps_failed(days_since=1):
    ext_cursor.execute(
        "SELECT * FROM swaps_failed WHERE started_at >= now() - INTERVAL "
        + str(days_since)
        + " DAY ORDER BY started_at;"
    )
    result = ext_cursor.fetchall()
    for x in result:
        row = swaps_failed_row()
        row.uuid = x[0]
        row.started_at = x[1]
        row.taker_coin = x[2]
        row.taker_amount = x[3]
        row.taker_error_type = x[4]
        row.taker_error_msg = x[5]
        row.taker_gui = x[6]
        row.taker_version = x[7]
        row.taker_pubkey = x[8]
        row.maker_coin = x[9]
        row.maker_amount = x[10]
        row.maker_error_type = x[11]
        row.maker_error_msg = x[12]
        row.maker_gui = x[13]
        row.maker_version = x[14]
        row.maker_pubkey = x[15]
        row.timestamp = int(x[1].replace(tzinfo=timezone.utc).timestamp())
        row.update()


if __name__ == "__main__":
    mysql = SqlUpdate("mysql")
    with mysql.engine.connect() as conn:
        r = conn.execute("Select * from swaps limit 1;")
        for i in r:
            logger.query(i)
