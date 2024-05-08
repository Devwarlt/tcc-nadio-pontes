import os
from traceback import format_exc
from numpy import ndarray
import pandas
import re
import subprocess
import requests
import json

from typing import Any, Dict, List

from splinter.driver import DriverAPI
from settings import Settings
from logging import fatal, info, warning
from selenium.webdriver import FirefoxOptions
from splinter import Browser
from http import HTTPStatus
from http.client import responses
from utils import format_stacktrace


__NOT_SET: str = "<ARGUMENT NOT SET>"
__REGEX_PATTERN_FILENAME: str = r"(CARGA_ENERGIA_)[0-9]{4,}"


def fetch_remote_data() -> None:
    open_data_ons_settings: Dict[str, Any] = Settings.CONFIG["open_data_ons"]
    source_dir: str = open_data_ons_settings.get("download_dir", __NOT_SET)
    if not os.path.exists(source_dir):
        os.mkdir(source_dir)

    source_url: str = open_data_ons_settings.get("url", __NOT_SET)
    bot = __configure_browser_automation_bot()
    bot.visit(source_url)

    csv_links: List[str] = [
        link["href"] for link in bot.find_by_xpath('//a[contains(@href, ".csv")]')
    ]

    bot.quit()

    info(f'Downloading CSV files to path "{source_dir}":')

    for csv_link in csv_links:
        csv_filename: str = re.search(
            f"{__REGEX_PATTERN_FILENAME}(.csv)", csv_link
        ).group()
        response: requests.Response = requests.get(csv_link)
        if response.status_code == HTTPStatus.OK:
            info(
                f"\t* [{response.status_code}] - {responses[response.status_code]} "
                + f'Successfully downloaded file: "{csv_filename}"'
            )
            with open(os.path.join(source_dir, csv_filename), "w") as csv_file:
                csv_file.write(response.content.decode("utf8"))
        else:
            warning(
                f"\t* [{response.status_code}] - {responses[response.status_code]} "
                + f'Unable to download file: "{csv_filename}"'
            )

    csv_file_paths: List[str] = [
        os.path.join(source_dir, file)
        for file in os.listdir(source_dir)
        if file.endswith(".csv")
    ]
    for csv_file_path in csv_file_paths:
        load_dataframe: pandas.DataFrame = pandas.read_csv(csv_file_path, sep=";")
        load_subsystem_ids: ndarray = load_dataframe["id_subsistema"].unique()
        load_subsystem_names: ndarray = load_dataframe["nom_subsistema"].unique()
        load_entries: List[Dict[str, str | str | List[str, float]]] = {}

        for load_subsystem_id, load_subsystem_name in zip(
            load_subsystem_ids, load_subsystem_names
        ):
            load_entries.append(
                {
                    "id": load_subsystem_id,
                    "nome": load_subsystem_name,
                    "registros": [
                        # "data": DATETIME -- 'din_instante'
                        # "carga" FLOAT -- 'val_cargaenergiamwmed'
                    ],
                }
            )

        load_instant_records: ndarray = load_dataframe["din_instante"].unique()
        json_filename: str = (
            f"{re.search(__REGEX_PATTERN_FILENAME, csv_file_path).group()}.json"
        )

        for load_instant_record in load_instant_records:
            instant_dataframe: pandas.DataFrame = load_dataframe.loc[
                load_dataframe["din_instante"] == load_instant_record
            ]
            for load_entry in load_entries:
                load_subsystem_id, load_subsystem_name, load_records = load_entry
                load_value_datafield: pandas.DataFrame = instant_dataframe.loc[
                    instant_dataframe["id_subsistema"] == load_entry[load_subsystem_id]
                ]["val_cargaenergiamwmed"]
                load_value: float = 0
                try:
                    if len(load_value_datafield) > 0:
                        load_value = float(load_value_datafield.iloc[0])
                        if pandas.isna(load_value):
                            continue

                        load_entry[load_records].append(
                            {"data": load_instant_record, "energia": load_value}
                        )
                    else:
                        continue
                except:
                    fatal(
                        format_stacktrace(
                            text="Unexpected data processing behavior.",
                            args={
                                "File": json_filename,
                                "Data": instant_dataframe.to_json(),
                                "Data (type)": type(load_value_datafield),
                                "Stacktrace": format_exc(),
                            },
                        )
                    )

        with open(os.path.join(source_dir, json_filename), "w") as json_file:
            info(f"Successfully created file: {json_filename}")
            json_file.write(json.dumps(load_entries, indent=True))


def __configure_browser_automation_bot() -> DriverAPI:
    web_scrapping_settings: Dict[str, Any] = Settings.CONFIG["web_scrapping"]
    geckodriver_options: FirefoxOptions = FirefoxOptions()
    geckodriver_options.binary_location = web_scrapping_settings.get(
        "binary_location", __NOT_SET
    )
    bot_options: Dict[str, Any] = {
        "driver_name": web_scrapping_settings.get("driver_name", __NOT_SET),
        "headless": web_scrapping_settings.get("headless", __NOT_SET),
        "options": geckodriver_options,
    }
    return Browser(**bot_options)