import os
import sys

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.coins import Coins
from lib.cache import reset_cache_files
import util.memcache as memcache

os.environ["IS_TESTING"] = "True"
reset_cache_files()
