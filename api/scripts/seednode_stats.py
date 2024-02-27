#!/usr/bin/env python3
import os
import sys
import sqlite3
import argparse

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.seednodes import seednode
from lib.dex_api import DexAPI
from util.logger import logger

if __name__ == "__main__":
    desc = "Register/deregister notary seed nodes or migrate stats from MM2.db to pgsql db."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--register", action="store_true", help="Register a notary Peer ID")
    parser.add_argument("--register_all", action="store_true", help="Register all notary Peer IDs")
    parser.add_argument("--deregister", action="store_true", help="Deregister a notary Peer ID")
    parser.add_argument("--deregister_all", action="store_true", help="Deregister all notary Peer IDs")
    parser.add_argument("--show", action="store_true", help="Display registered notaries")
    parser.add_argument(
        "--migrate_stats",
        action="store_true",
        help="Migrates MM2.db seednode stats to PgSql",
    )
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()

    if args.migrate_stats:
        source_data = seednode.get_seednode_stats()
        # TODO: Write to pgsql
        pass
    elif args.show:
        # TODO: add function to print to console
        data = seednode.latest_data
        for row in data:
            logger.info(row)
    elif args.register_all:
        dex = DexAPI()
        for notary in seednode.notary_seednodes:
            domain = seednode.notary_seednodes[notary]["domain"]
            peer_id = seednode.notary_seednodes[notary]["peer_id"]
            logger.info(dex.add_seednode_for_stats(notary, domain, peer_id))
        
    elif args.deregister_all:
        dex = DexAPI()
        for notary in seednode.notary_seednodes:
            logger.info(dex.remove_seednode_from_stats(notary))

    elif args.register:
        dex = DexAPI()
        notary = input("Notary: ")
        domain = input("Domain: ")
        peer_id = input("Peer Id: ")
        logger.info(dex.add_seednode_for_stats(notary, domain, peer_id))
        
    elif args.deregister:
        dex = DexAPI()
        notary = input("Notary: ")
        # TODO: show selectable list
        dex.remove_seednode_from_stats(notary)
