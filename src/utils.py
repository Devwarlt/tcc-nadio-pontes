from datetime import datetime, timedelta
from string import Formatter
from typing import Any, Dict


DATETIME_FORMAT: str = "%Y-%m-%d %H:%M:%S"
TIMEZONE_DIFFERENCE: Dict[str, int] = {"hours": -3, "minutes": 0}
NOT_SET: str = "<ARGUMENT NOT SET>"

REGEX_PATTERN_FILENAME: str = r"(CARGA_ENERGIA_)[0-9]{4,}"
REGEX_PATTERN_FILENAME_YEAR: str = r"[0-9]{4,}"

# `os._exit(n)` exit codes:
#   - https://docs.python.org/3/library/os.html#os._exit
EX_OK: int = 0
EX_SOFTWARE: int = 70


def format_stacktrace(text: str, **kwargs: Dict[str, Any]) -> str:
    message: str = text
    args: Dict[str, Any] = kwargs.pop("args", {})
    if args:
        for key, value in args.items():
            message += f"\n- {key}: {value}"
    return message


def brt_now() -> datetime:
    return datetime.utcnow() + timedelta(**TIMEZONE_DIFFERENCE)


def strfdelta(tdelta, fmt="{D:02}d {H:02}h {M:02}m {S:02}s", inputtype="timedelta"):
    """
    Source: https://stackoverflow.com/a/42320260

    Convert a datetime.timedelta object or a regular number to a custom-
    formatted string, just like the stftime() method does for datetime.datetime
    objects.

    The fmt argument allows custom formatting to be specified.  Fields can
    include seconds, minutes, hours, days, and weeks.  Each field is optional.

    Some examples:
        '{D:02}d {H:02}h {M:02}m {S:02}s' --> '05d 08h 04m 02s' (default)
        '{W}w {D}d {H}:{M:02}:{S:02}'     --> '4w 5d 8:04:02'
        '{D:2}d {H:2}:{M:02}:{S:02}'      --> ' 5d  8:04:02'
        '{H}h {S}s'                       --> '72h 800s'

    The inputtype argument allows tdelta to be a regular number instead of the
    default, which is a datetime.timedelta object.  Valid inputtype strings:
        's', 'seconds',
        'm', 'minutes',
        'h', 'hours',
        'd', 'days',
        'w', 'weeks'
    """

    # Convert tdelta to integer seconds.
    if inputtype == "timedelta":
        remainder = int(tdelta.total_seconds())
    elif inputtype in ["s", "seconds"]:
        remainder = int(tdelta)
    elif inputtype in ["m", "minutes"]:
        remainder = int(tdelta) * 60
    elif inputtype in ["h", "hours"]:
        remainder = int(tdelta) * 3600
    elif inputtype in ["d", "days"]:
        remainder = int(tdelta) * 86400
    elif inputtype in ["w", "weeks"]:
        remainder = int(tdelta) * 604800

    f = Formatter()
    desired_fields = [field_tuple[1] for field_tuple in f.parse(fmt)]
    possible_fields = ("W", "D", "H", "M", "S")
    constants = {"W": 604800, "D": 86400, "H": 3600, "M": 60, "S": 1}
    values = {}
    for field in possible_fields:
        if field in desired_fields and field in constants:
            values[field], remainder = divmod(remainder, constants[field])
    return f.format(fmt, **values)