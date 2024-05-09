from typing import Any, Dict, Tuple
from yaml import safe_load


class Settings(object):
    CONFIG: Dict[str, Any] = {}

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]):
        raise SyntaxError("This is an utility class.")

    @staticmethod
    def load(path: str) -> None:
        with open(path, "r") as file:
            Settings.CONFIG = safe_load(file)