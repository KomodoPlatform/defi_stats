import time
from util.exceptions import NoDefaultForKeyError
from const import DEXAPI_8762_HOST, IS_TESTING


def arg_defaults():
    val_keys = {
        "false": ["reverse", "testing"],
        "true": ["wal", "order_by_mcap"],
        "none": ["source_url", "db_path", "db"],
        "now": ["end"],
        "ALL": ["netid"],
        "default_host": ["mm2_host"],
        "zero": ["trigger"],
        "kmd": ["coin"],
        "debug": ["loglevel"],
        "empty_string": ["msg"],
    }
    args = []
    for v in val_keys.values():
        args += v
    return {"val_keys": val_keys, "args": args}


def default_val(key: str):
    for val, keys in arg_defaults()["val_keys"].items():
        if key in keys:
            if val.lower() == "true":
                return True
            if val.lower() == "false":
                return False
            if val.lower() == "none":
                return None
            if val.lower() == "all":
                return "ALL"
            if val.lower() == "empty_string":
                return ""
            if val.lower() == "zero":
                return 0
            if val.lower() == "kmd":
                return "KMD"
            if val.lower() == "default_host":
                return DEXAPI_8762_HOST
            if val.lower() == "now":
                return int(time.time())
            else:
                return "debug"
    raise NoDefaultForKeyError(f"No default value for {key}!")  # pragma: no cover


def set_params(object: object(), kwargs: dict(), options: list()) -> None:
    # Set the defaults from object options if not already set
    try:
        if IS_TESTING:
            setattr(object, "testing", True)
        else:
            setattr(object, "testing", False)
        [setattr(object, k, v) for k, v in kwargs.items()]

        for arg in arg_defaults()["args"]:
            if arg in options:
                if getattr(object, arg, "unset") == "unset":
                    setattr(object, arg, default_val(arg))

    except Exception as e:  # pragma: no cover
        msg = "Setting default params failed!"
        return default_error(e.msg)
    msg = "Setting default params complete!"
    return default_result(msg=msg, loglevel="debug", ignore_until=10)


def default_error(
    e, msg=None, loglevel="error", ignore_until=0, data=None
):  # pragma: no cover
    if msg is None:
        msg = e
    else:
        msg = f"{e}: {msg}"
    r = {
        "result": "error",
        "message": msg,
        "error": str(type(e)),
        "loglevel": loglevel,
        "ignore_until": ignore_until,
        "data": data,
    }
    return r


def default_result(
    data=None, msg=None, loglevel="debug", ignore_until=0
):  # pragma: no cover
    r = {
        "result": "success",
        "message": msg,
        "data": data,
        "loglevel": loglevel,
        "ignore_until": ignore_until,
    }
    return r
