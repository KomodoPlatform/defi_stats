import time
from util.exceptions import NoDefaultForKeyError


def arg_defaults():
    val_keys = {
        "false": ["reverse", "testing"],
        "true": ["wal", "exclude_unpriced", "order_by_mcap"],
        "none": ["endpoint", "source_url", "db_path"],
        "now": ["end"],
        "all": ["netid"],
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
                return "http://127.0.0.1"
            if val.lower() == "now":
                return int(time.time())
            else:
                return "debug"
    raise NoDefaultForKeyError(f"No default value for {key}!")  # pragma: no cover


def set_params(object: object(), kwargs: dict(), options: list()) -> None:
    # Set the defaults from object options if not already set
    try:
        for arg in arg_defaults()["args"]:
            if arg in options:
                if getattr(object, arg, "unset") == "unset":
                    setattr(object, arg, default_val(arg))
        # Then process kwargs
        [setattr(object, k, v) for k, v in kwargs.items()]

    except Exception as e:  # pragma: no cover
        msg = "Setting default params failed!"
        return default_error(e.msg)
    msg = "Setting default params complete!"
    return default_result(msg=msg, loglevel="debug", ignore_until=10)


def default_error(e, msg=None, loglevel="error", ignore_until=0):  # pragma: no cover
    if msg is None:
        msg = e
    else:
        msg = f"{e}: {msg}"
    r = {"result": "error", "message": msg, "error": str(type(e)), "loglevel": loglevel}
    if msg is not None:
        r.update({"message": msg})
    if ignore_until is not None:
        r.update({"ignore_until": ignore_until})
    return r


def default_result(
    msg="No Message", loglevel="debug", ignore_until=0
):  # pragma: no cover
    r = {"result": "success", "message": msg, "loglevel": loglevel}
    if msg is not None:
        r.update({"message": msg})
    if ignore_until is not None:
        r.update({"ignore_until": ignore_until})
    return r
