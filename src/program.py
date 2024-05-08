from concurrent.futures import Future, ThreadPoolExecutor
from logging import INFO, Formatter, basicConfig, critical, info
from threading import Event
from time import sleep
from traceback import format_exc
from typing import Any, Dict, Literal
from fastapi import FastAPI
from urllib3 import disable_warnings
from uvicorn import run
from settings import Settings
from routers import root_router
from utils import format_stacktrace


# TODO!!!
def __populate_database(event_flag: Event) -> None:
    while not event_flag.is_set():
        print("a")
        sleep(1)


if __name__ == "__main__":
    log_fmt: Formatter = Formatter(
        "%(asctime)s,%(msecs)-3d - %(levelname)-8s => " "%(message)s"
    )
    log_config: Dict[str, Any] = {
        "format": vars(log_fmt).get("_fmt"),
        "datefmt": "%Y-%m-%d %H:%M:%S",
        "level": INFO,
    }
    basicConfig(**log_config)
    disable_warnings()

    exit_code: Literal[0, 1] = 0

    info("Loading application settings...")

    Settings.load("../settings.yaml")

    with ThreadPoolExecutor(max_workers=1) as thread_executor:
        populate_database_event: Event = Event()
        populate_thread: Future = thread_executor.submit(
            __populate_database, populate_database_event
        )

        try:
            app: FastAPI = FastAPI()
            app.include_router(root_router)
            run(app, **Settings.CONFIG["app"])
        except Exception:
            exit_code = 1
            critical(
                format_stacktrace(
                    text="Unexpected process behaviour.",
                    args={"Stacktrace": format_exc()},
                )
            )
        finally:
            populate_thread.cancel()
            populate_database_event.set()

            info(f"Exit code: {exit_code}")
            exit(exit_code)