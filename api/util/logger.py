#!/usr/bin/env python3
import logging


class CustomFormatter(logging.Formatter):
    white = "\x1b[m"
    darkgrey = "\x1b[2m"
    italic_white = "\x1b[3m"
    underline_white = "\x1b[4m"
    lightgrey = "\x1b[5m"
    debug = "\x1b[30;1m"
    black = "\x1b[30m"
    error = "\x1b[31m"
    red = "\x1b[31m"
    green = "\x1b[32m"
    gold = "\x1b[33m"
    blue = "\x1b[34m"
    purple = "\x1b[35m"
    cyan = "\x1b[36m"
    lightgrey = "\x1b[37m"
    table = "\x1b[37m"
    midgrey = "\x1b[90m"
    lightred = "\x1b[91m"
    lightgreen = "\x1b[92m"
    othergreen = "\x1b[32m"
    yellow = "\x1b[93m"
    lightblue = "\x1b[96m"
    status = "\x1b[94m"
    pink = "\x1b[95m"
    lightcyan = "\x1b[96m"
    grey = "\x1b[38;20m"
    yellow2 = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = (
        "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
    )
    datefmt = "%d-%b-%y %H:%M:%S"

    FORMATS = {
        logging.DEBUG: black + format + reset,
        logging.INFO: lightgreen + format + reset,
        logging.WARNING: red + format + reset,
        logging.ERROR: lightred + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        if record.levelname == "STOPWATCH":
            log_fmt = (
                self.yellow
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "LOOP":
            log_fmt = (
                self.yellow
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "QUERY":
            log_fmt = (
                self.gold
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "DEXRPC":
            log_fmt = (
                self.lightcyan
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "MUTED":
            log_fmt = (
                self.midgrey
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "IMPORTED":
            log_fmt = (
                self.purple
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "UPDATED":
            log_fmt = (
                self.lightblue
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        else:
            log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def addLoggingLevel(levelName, levelNum, methodName=None):
    # From https://stackoverflow.com/questions/2183233/
    # how-to-add-a-custom-loglevel-to-pythons-logging-facility/

    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
        raise AttributeError("{} already defined in logging module".format(levelName))
    if hasattr(logging, methodName):
        raise AttributeError("{} already defined in logging module".format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
        raise AttributeError("{} already defined in logger class".format(methodName))

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)


logger = logging.getLogger("dexstats_app")
# create console handler with a higher log level
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logger.addHandler(handler)

# Shows DB imports
addLoggingLevel("IMPORTED", logging.DEBUG + 9)
logger.setLevel("IMPORTED")

# Shows cache updates
addLoggingLevel("UPDATED", logging.DEBUG + 8)
logger.setLevel("UPDATED")

# Shows database req/resp
addLoggingLevel("QUERY", logging.DEBUG + 7)
logger.setLevel("QUERY")

# Shows dex api req/resp
addLoggingLevel("DEXRPC", logging.DEBUG + 6)
logger.setLevel("DEXRPC")

# Shows cache loop updates
addLoggingLevel("LOOP", logging.DEBUG + 5)
logger.setLevel("LOOP")

# Shows time taken to run functions
addLoggingLevel("STOPWATCH", logging.DEBUG + 5)
logger.setLevel("STOPWATCH")

# Shows generally ignorable errors, e.g. CoinConfigNotFound
addLoggingLevel("MUTED", logging.DEBUG - 1)
logger.setLevel("MUTED")
