from datetime import datetime, timedelta
from typing import Any, Dict


def format_stacktrace(text: str, **kwargs: Dict[str, Any]) -> str:
    message: str = text
    args: Dict[str, Any] = kwargs.pop("args", {})
    if args:
        for key, value in args.items():
            message += f"\n- {key}: {value}"
    return message


def brt_now() -> datetime:
    return datetime.utcnow() - timedelta(hours=3)