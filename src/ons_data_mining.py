from datetime import datetime, timedelta, timezone
from traceback import format_exc
from numpy import ndarray
from typing import Any, Dict, List
from db import MariaDbUtils, Report
from logging import fatal, info, warning
from selenium.webdriver import FirefoxOptions
from splinter import Browser
from http import HTTPStatus
from http.client import responses
from utils import (
    NOT_SET,
    REGEX_PATTERN_FILENAME,
    REGEX_PATTERN_FILENAME_YEAR,
    TIMEZONE_DIFFERENCE,
    format_stacktrace,
    brt_now,
    strfdelta,
)

import os
import pandas
import re
import requests
import json
import glob


def cleanup_all_downloaded_resources(source_dir: str) -> None:
    info(f'Removing all temporarily resources from path "{source_dir}"...')

    for file in glob.glob(os.path.join(source_dir, "*")):
        os.remove(file)


def update_all_open_data_reports(source_dir: str) -> None:
    info(f"Synchronizing fetched reports and updating internal data...")

    json_file_paths: List[str] = [
        os.path.join(source_dir, file)
        for file in os.listdir(source_dir)
        if file.endswith(".json")
    ]
    for json_file_path in json_file_paths:
        with open(json_file_path, "r") as json_file:
            json_raw_content: str = json_file.read()
            filename_year: int = int(
                re.search(REGEX_PATTERN_FILENAME_YEAR, json_file_path).group()
            )
            load_entries: List[Dict[str, str | str | List[Dict[str, str | float]]]] = (
                json.loads(json_raw_content)
            )

            info(f'Updating reports from year "{filename_year}"...')

            for load_entry in load_entries:
                subsystem_id, subsystem_name, subsystem_load_records = (
                    load_entry.values()
                )
                subsystem_reports: List[Report] = [
                    Report(subsystem_id, *subsystem_load_record.values())
                    for subsystem_load_record in subsystem_load_records
                ]
                count_entries: int = subsystem_reports.__len__()

                info(
                    f"\t* [{subsystem_name} - year: {filename_year}] Entries: {count_entries}"
                )

                if MariaDbUtils.add_reports(subsystem_reports):
                    info("\t* Successfully added all reports.")
                else:
                    fatal("\t* Unable to add data subsystem reports!")


def fetch_open_data_reports(
    open_data_ons_settings: Dict[str, Any],
    web_scrapping_settings: Dict[str, Any],
    source_dir: str,
) -> None:
    fetch_period_threshold_settings: Dict[str, Any] = web_scrapping_settings[
        "fetch_period_threshold"
    ]
    current_date: datetime = brt_now()
    latest_instant_record: datetime = MariaDbUtils.fetch_latest_instant_record()
    if (
        current_date.day
        > (latest_instant_record + timedelta(**fetch_period_threshold_settings)).day
    ):
        next_check_eta: timedelta = current_date - (
            latest_instant_record + timedelta(**fetch_period_threshold_settings)
        )
        current_date = current_date.replace(tzinfo=timezone.utc)
        timezone_hours, timezone_minutes = TIMEZONE_DIFFERENCE.values()
        info(
            "All reports are up to date, next check within:"
            + f' {strfdelta(next_check_eta, "{H:02}h {M:02}m {S:02}s")}'
            + f" {timezone_hours:+03d}:{timezone_minutes:02d}"
            + f' {current_date.strftime("%Z")}'
        )
        return

    info("Pending reports to update, fetching remote Open Data servers...")

    geckodriver_options: FirefoxOptions = FirefoxOptions()
    geckodriver_options.binary_location = web_scrapping_settings.get(
        "binary_location", NOT_SET
    )
    bot_options: Dict[str, Any] = {
        "driver_name": web_scrapping_settings.get("driver_name", NOT_SET),
        "headless": web_scrapping_settings.get("headless", NOT_SET),
        "options": geckodriver_options,
    }
    source_url: str = open_data_ons_settings.get("url", NOT_SET)
    bot = Browser(**bot_options)
    bot.visit(source_url)

    csv_links: List[str] = [
        link["href"] for link in bot.find_by_xpath('//a[contains(@href, ".csv")]')
    ]

    bot.quit()

    info(f'Downloading CSV files to path "{source_dir}"...')

    distinct_instant_record_years: List[int] = (
        MariaDbUtils.fetch_distinct_instant_record_years()
    )
    if not distinct_instant_record_years:
        distinct_instant_record_years = []

    for csv_link in csv_links:
        csv_filename: str = re.search(
            f"{REGEX_PATTERN_FILENAME}(.csv)", csv_link
        ).group()
        csv_filename_year: int = int(
            re.search(REGEX_PATTERN_FILENAME_YEAR, csv_filename).group()
        )
        if csv_filename_year in distinct_instant_record_years:
            continue

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
        load_entries: List[Dict[str, str | str | List[str, float]]] = []

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
            f"{re.search(REGEX_PATTERN_FILENAME, csv_file_path).group()}.json"
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
