from time import *
from typing import *
from uvicorn import *
from fastapi import *
from logging import *
from urllib3 import *
from traceback import *
from routers import *


def __format_stacktrace(text: str, **kwargs: Dict[str, Any]) -> str:
    message: str = text
    args: Dict[str, Any] = kwargs.pop('args', {})
    if args:
        for key, value in args.items():
            message += f"\n- {key}: {value}"
    return message


if __name__ == "__main__":
    log_fmt: Formatter = Formatter(
        '%(asctime)s,%(msecs)-3d - %(levelname)-8s => '
        '%(message)s'
    )
    log_config: Dict[str, Any] = {
        'format': vars(log_fmt).get("_fmt"),
        'datefmt': '%Y-%m-%d %H:%M:%S',
        'level': INFO
    }
    basicConfig(**log_config)
    disable_warnings()

    exit_code: Literal[0, 1] = 0

    try:
        __api: FastAPI = FastAPI()
        __api.include_router(root_router)
        run(
            app=__api,
            host="localhost",
            port=5000
        )
    except Exception:
        exit_code = 1
        critical(
            __format_stacktrace(
                text="Unexpected process behaviour.",
                args={'Stacktrace': format_exc()}
            )
        )
    finally:
        info(f"Exit code: {exit_code}")
        sleep(1)
        exit(exit_code)
