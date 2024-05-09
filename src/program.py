from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from logging import INFO, Formatter, basicConfig, critical, info
from os import _exit
from threading import Event
from time import sleep
from traceback import format_exc
from typing import Any, Dict
from fastapi import FastAPI
from urllib3 import disable_warnings
from uvicorn import run
from settings import Settings
from routers import root_router
from utils import format_stacktrace
from ons_data_mining import trigger_bot_routine

# `os._exit(n)` exit codes:
#   - https://docs.python.org/3/library/os.html#os._exit
EX_OK: int = 0
EX_SOFTWARE: int = 70


def __update_open_data(event_flag: Event) -> None:
    web_scrapping_settings: Dict[str, Any] = Settings.CONFIG["web_scrapping"]
    fetch_wait_time_settings: Dict[str, Any] = web_scrapping_settings["fetch_wait_time"]
    while not event_flag.is_set():
        trigger_bot_routine(web_scrapping_settings)
        sleep(timedelta(**fetch_wait_time_settings).total_seconds())


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

    exit_status_flag: int = EX_OK

    info("Loading application settings...")

    Settings.load("../settings.yaml")

    with ThreadPoolExecutor(max_workers=1) as thread_executor:
        update_open_data_event: Event = Event()
        update_open_data_thread: Future = thread_executor.submit(
            __update_open_data, update_open_data_event
        )

        try:
            app: FastAPI = FastAPI()
            app.include_router(root_router)
            run(app, **Settings.CONFIG["app"])
        except:
            exit_status_flag = EX_SOFTWARE
            critical(
                format_stacktrace(
                    text="Unexpected process behaviour.",
                    args={"Stacktrace": format_exc()},
                )
            )
        finally:
            update_open_data_thread.cancel()
            update_open_data_event.set()

            info(f"Exit status: {exit_status_flag}")
            _exit(exit_status_flag)