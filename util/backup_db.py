#!/usr/bin/env python3
import sqlite3
import argparse

"""Used to backup MM2.db files on source servers prior to exfiltration with rsync"""


def progress(status, remaining, total):
    print(f"Copied {total-remaining} of {total} pages...")


def backup_db(src_db_path, dest_db_path):
    src = sqlite3.connect(src_db_path)
    dest = sqlite3.connect(dest_db_path)
    with dest:
        create_swap_stats_table(dest.cursor())
        src.backup(dest, pages=1, progress=progress)
    dest.close()
    src.close()


def create_swap_stats_table(cursor):
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS
            stats_swaps (
                id INTEGER NOT NULL PRIMARY KEY,
                maker_coin VARCHAR(255) NOT NULL,
                taker_coin VARCHAR(255) NOT NULL,
                uuid VARCHAR(255) NOT NULL UNIQUE,
                started_at INTEGER NOT NULL,
                finished_at INTEGER NOT NULL,
                maker_amount DECIMAL NOT NULL,
                taker_amount DECIMAL NOT NULL,
                is_success INTEGER NOT NULL,
                maker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
                maker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
                taker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
                taker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
                maker_coin_usd_price DECIMAL,
                taker_coin_usd_price DECIMAL,
                maker_pubkey VARCHAR(255) DEFAULT '',
                taker_pubkey VARCHAR(255) DEFAULT ''
            );
        """
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dest", type=str, required=True)
    args = parser.parse_args()
    backup_db(args.src, args.dest)
