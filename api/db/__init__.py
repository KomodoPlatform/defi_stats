# from db.sqlitedb import SqliteDB
import os

print("Init DBs...")
if "IS_TESTING" in os.environ:
    testing = True
else:
    testing = False
