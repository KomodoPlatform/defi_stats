#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
import db.sqldb as db
from util.logger import logger


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date format. Please use YYYY-M-D format.")

def main():
    today = datetime.now().date().strftime("%Y-%m-%d")

    desc = 'Import swaps between two dates in the format YYYY-M-D.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--start', type=parse_date, help='Start date in YYYY-M-D format', default="2019-9-1")
    parser.add_argument('--end', type=parse_date, help='End date in YYYY-M-D format', default=today)
    parser.add_argument('--reset_table', action='store_true', help='Warning: This will dump the table, then recreate it empty.')

    args = parser.parse_args()
    logger.info(f"Importing swaps between {args.start} and {args.end}...")
        
    DB = db.SqlSource()
    if args.reset_table:
        DB.reset_defi_stats_table()
    DB.import_swaps(start_dt=args.start, end_dt=args.end)

if __name__ == "__main__":
    main()
