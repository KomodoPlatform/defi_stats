import os
from pymemcache.client.base import PooledClient


def pytest_sessionfinish(session, exitstatus):
    """
    This function is called at the end of the test session.
    """

    MEMCACHE = PooledClient(("localhost", 11211), timeout=10, max_pool_size=250)
    print("The test session has finished, and memcache flushed.")

    MEMCACHE.flush_all()
    os.environ["IS_TESTING"] = "False"
