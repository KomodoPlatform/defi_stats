class ApiKeyNotFoundException(Exception):
    "Raised when a required API key is not found in the .env file"
    pass


class CoinNotFoundException(Exception):
    "Raised when a coin is not found in the coins_config"
    pass


class CoinWalletOnlyException(Exception):
    "Raised when a coin is wallet_only in the coins_config"
    pass


class NoDefaultForKeyError(Exception):
    """
    Raised when attempting to get the default value of a key
    which has no default value.
    """

    pass


class RequiredQueryParamMissing(Exception):
    "Raised when a required database query param is missing"
    pass


class InvalidParamCombination(Exception):
    "Raised when two or more mutually exclusive params were selected"
    def __init__(self, msg: str, status_code: int = 400):
        self.status_code = 400
        self.msg = msg
        self.name = "InvalidParamCombination"


class UuidNotFoundException(Exception):
    "Raised when a UUID is not found in the database"
    pass


class CacheFilenameNotFound(Exception):
    """
    Raised when a filename for a cache item is not found
    in the 'File' class"
    """

    pass


class CacheItemNotFound(Exception):
    "Raised when a cache item is not found in the 'Cache' class"
    pass


class DataStructureError(Exception):
    "Raised when a returned data is an unexpected structure"
    pass


class BadPairFormatError(Exception):
    def __init__(self, msg: str, status_code: int = 400):
        self.status_code = 400
        self.msg = msg
        self.name = "BadPairFormatError"
