# from db.sqlitedb import SqliteDB
import os
from db.sqlitedb_merge import SqliteMerge
from lib.cache_load import load_coins_config, load_gecko_source

print("Init DBs...")
if "IS_TESTING" in os.environ:
    testing = True
else:
    testing = False
coins_config = load_coins_config(testing=testing)
gecko_source = load_gecko_source(testing=testing)
merge = SqliteMerge(testing=testing)
merge.init_dbs(coins_config=coins_config, gecko_source=gecko_source)
