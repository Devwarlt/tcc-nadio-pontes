from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from logging import INFO, Formatter, basicConfig, critical, info
from threading import Event
from time import sleep
from traceback import format_exc
from typing import Any, Dict
from fastapi import FastAPI
from urllib3 import disable_warnings
from uvicorn import run
from settings import Settings
from routers import root_router
from utils import EX_OK, EX_SOFTWARE, format_stacktrace
from ons_data_mining import (
    cleanup_all_downloaded_resources,
    fetch_open_data_reports,
    update_all_open_data_reports,
)

import os


def __open_data_bot_dynamic_sync(event_flag: Event) -> None:
    open_data_ons_settings: Dict[str, Any] = Settings.CONFIG["open_data_ons"]
    source_dir: str = open_data_ons_settings["download_dir"]
    source_dir = os.path.join(os.getcwd(), source_dir)
    if not os.path.exists(source_dir):
        os.mkdir(source_dir)

    web_scrapping_settings: Dict[str, Any] = Settings.CONFIG["web_scrapping"]
    fetch_wait_time_settings: Dict[str, Any] = web_scrapping_settings["fetch_wait_time"]
    while not event_flag.is_set():
        if os.listdir(source_dir) != []:
            cleanup_all_downloaded_resources(source_dir)

        fetch_open_data_reports(
            open_data_ons_settings, web_scrapping_settings, source_dir
        )
        update_all_open_data_reports(source_dir)
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
        open_data_bot_event: Event = Event()
        open_data_bot_thread: Future = thread_executor.submit(
            __open_data_bot_dynamic_sync, open_data_bot_event
        )

        try:
            root_router.responses = Settings.CONFIG["api_responses"]

            app: FastAPI = FastAPI()
            app.include_router(root_router)
            run(app, **Settings.CONFIG["app"])
        except KeyboardInterrupt:
            pass
        except:
            exit_status_flag = EX_SOFTWARE
            critical(
                format_stacktrace(
                    text="Unexpected process behaviour.",
                    args={"Stacktrace": format_exc()},
                )
            )
        finally:
            open_data_bot_thread.cancel()
            open_data_bot_event.set()

            info(f"Exit status: {exit_status_flag}")
            os._exit(exit_status_flag)