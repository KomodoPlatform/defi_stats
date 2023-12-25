#!/usr/bin/env python3
import time
import inspect
import logging
from util.templates import Templates

templates = Templates()


class CustomFormatter(logging.Formatter):
    white = "\x1b[m"
    italic_white = "\x1b[3m"
    underline_white = "\x1b[4m"

    muted = "\x1b[38;2;150;200;150m"
    black = "\x1b[30m"
    lightgrey = "\x1b[37m"
    grey = "\x1b[38;20m"
    midgrey = "\x1b[90m"

    gold = "\x1b[33m"
    yellow = "\x1b[93m"
    yellow2 = "\x1b[33;20m"
    yellow3 = "\x1b[33;1m"
    lightyellow = "\x1b[38;2;250;250;150m"

    green = "\x1b[32m"
    mintgreen = "\x1b[38;2;150;250;150m"
    lightgreen = "\x1b[92m"
    othergreen = "\x1b[32;1m"
    drabgreen = "\x1b[38;2;150;200;150m"

    skyblue = "\x1b[38;2;150;250;250m"
    iceblue = "\x1b[38;2;59;142;200m"
    blue = "\x1b[34m"
    magenta = "\x1b[35m"
    purple = "\x1b[38;2;150;150;250m"
    cyan = "\x1b[36m"

    lightblue = "\x1b[96m"
    lightcyan = "\x1b[96m"

    pink = "\x1b[95m"
    lightred = "\x1b[91m"
    red = "\x1b[31;20m"
    red2 = "\x1b[31m"
    bold_red = "\x1b[31;1m"

    table = "\x1b[37m"
    status = "\x1b[94m"
    debug = "\x1b[30;1m"
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
        elif record.levelname == "QUERY":
            log_fmt = (
                self.lightyellow
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "LOOP":
            log_fmt = (
                self.purple
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "MUTED":
            log_fmt = (
                self.muted
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "CALC":
            log_fmt = (
                self.lightcyan
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "IMPORTED":
            log_fmt = (
                self.mintgreen
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "DEXRPC":
            log_fmt = (
                self.skyblue
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            )
        elif record.levelname == "UPDATED":
            log_fmt = (
                self.lightgreen
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
            )
        elif record.levelname == "SAVE":
            log_fmt = (
                self.drabgreen
                + "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"
                + self.reset
            ) + self.reset
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

# Shows cache loop updates
addLoggingLevel("CALC", logging.DEBUG + 4)
logger.setLevel("CALC")

# Shows cache loop updates
addLoggingLevel("SAVE", logging.DEBUG + 3)
logger.setLevel("SAVE")

# Shows time taken to run functions
addLoggingLevel("STOPWATCH", logging.DEBUG + 2)
logger.setLevel("STOPWATCH")

# Shows generally ignorable errors, e.g. CoinConfigNotFound
addLoggingLevel("MUTED", logging.DEBUG - 1)
logger.setLevel("MUTED")


class StopWatch:
    def __init__(self, start_time, **kwargs) -> None:
        self.start_time = start_time
        self.get_stopwatch(**kwargs)

    def get_stopwatch(self, **kwargs):
        options = [
            "testing",
            "trigger",
            "context",
            "updated",
            "query",
            "imported",
            "error",
            "warning",
            "debug",
            "dexrpc",
            "muted",
            "info",
            "loop",
            "calc",
            "save",
        ]
        templates.set_params(self, kwargs, options)
        if self.trigger == 0:
            self.trigger = 10
            if self.updated or self.imported or self.query or self.dexrpc:
                self.trigger = 5
            if self.error or self.debug or self.warning or self.loop:
                self.trigger = 0
        self.trigger = 0
        duration = int(time.time()) - int(self.start_time)
        if duration > self.trigger:
            if self.context is None:
                self.context = get_trace(inspect.stack()[1], "guessed action")
            if "|" in self.context:
                x = self.context.split("|")
                self.context = f"{x[0]:^40}|{x[1]:>80}"
            msg = f"[{duration:>4} sec] [{self.context}]"
            if self.updated:
                logger.updated(f"    {msg}")
            elif self.error:
                logger.error(f"      {msg}")
            elif self.imported:
                logger.imported(f"   {msg}")
            elif self.query:
                logger.query(f"      {msg}")
            elif self.debug:
                logger.debug(f"      {msg}")
            elif self.warning:
                logger.warning(f"    {msg}")
            elif self.muted:
                logger.muted(f"      {msg}")
            elif self.info:
                logger.info(f"       {msg}")
            elif self.calc:
                logger.calc(f"       {msg}")
            elif self.loop:
                logger.loop(f"       {msg}")
            elif self.save:
                logger.save(f"       {msg}")
            else:
                logger.stopwatch(f"  {msg}")


def get_trace(stack, error=None):
    context = {
        "stack": {
            "function": stack.function,
            "file": stack.filename,
            "lineno": stack.lineno,
        }
    }
    if error is not None:
        context.update({"error": error})
    return context


def show_pallete():
    logger.info("info")
    logger.debug("debug")
    logger.warning("warning")
    logger.error("error")
    logger.critical("critical")
    logger.updated("updated")
    logger.imported("imported")
    logger.save("save")
    logger.calc("calc")
    logger.loop("loop")
    logger.muted("muted")
    logger.query("query")
