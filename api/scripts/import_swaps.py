#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import date, datetime

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
from const import RESET_TABLE
import db.sqldb as db


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date format. Please use D-M-YYYY format.")

def main():
    today = datetime.now().date().strftime("%d-%m-%Y")

    desc = 'Import swaps between two dates in the format D-M-YYYY.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--start', type=parse_date, metavar='start_date', help='Start date in D-M-YYYY format', default="1-9-2019")
    parser.add_argument('--end', type=parse_date, metavar='end_date', help='End date in D-M-YYYY format', default=today)
    parser.add_argument('--reset_table', action='store_true', help='Warning: This will dump the table, then recreate it empty.')

    args = parser.parse_args()

    print("Start date:", args.start_date)
    print("End date:", args.end_date)
        
    DB = db.SqlSource()
    if args.reset_table:
        DB.reset_defi_stats_table()
    DB.import_swaps(start_dt=args.start_date, end_dt=args.end_date)

if __name__ == "__main__":
    main()
