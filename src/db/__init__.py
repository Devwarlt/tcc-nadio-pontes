from datetime import datetime
from logging import warning
from typing import Any, Dict, List, Sequence, Tuple
from mariadb import Connection, Cursor
from settings import Settings
from utils import DATETIME_FORMAT, brt_now

import re


class Report(object):
    def __init__(
        self,
        subsystem_id: str,
        instant_record: datetime | str,
        instant_load_following: float,
    ):
        self.__subsystem_id: str = subsystem_id
        self.__instant_record: datetime = datetime.min

        if isinstance(instant_record, datetime):
            self.__instant_record = instant_record
        else:
            try:
                self.__instant_record = datetime.strptime(
                    instant_record, DATETIME_FORMAT
                )
            except ValueError:
                self.__instant_record = datetime.strptime(
                    f"{instant_record} 00:00:00", DATETIME_FORMAT
                )

        self.__instant_load_following: float = instant_load_following

    @property
    def subsystem_id(self) -> str:
        return self.__subsystem_id

    @subsystem_id.setter
    def subsystem_id(self, value: str) -> None:
        self.__subsystem_id = value

    @property
    def instant_record(self) -> datetime:
        return self.__instant_record

    @instant_record.setter
    def instant_record(self, value: datetime) -> None:
        self.__instant_record = value

    @property
    def instant_load_following(self) -> float:
        return self.__instant_load_following

    @instant_load_following.setter
    def instant_load_following(self, value: float) -> None:
        self.__instant_load_following = value

    def serialize_data(self) -> Tuple[str, datetime, float]:
        return (
            self.__subsystem_id,
            self.__instant_record,
            self.__instant_load_following,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "subsystem_id": self.__subsystem_id,
            "instant_record": self.__instant_record.strftime(DATETIME_FORMAT),
            "instant_load_following": self.__instant_load_following,
        }


class MariaDbUtils(object):
    def __init__(self, *args: Tuple[Any, ...]):
        raise SyntaxError("This is an utility class.")

    @staticmethod
    def add_report(report: Report) -> bool:
        with MariaDb() as mariadb:
            return mariadb.execute(
                query="""
                INSERT INTO `sin_subsystems_reports` (
                    `subsystem_id`, `instant_record`,
                    `instant_load_following`
                ) VALUES (?, ?, ?)
                """,
                data=report.serialize_data(),
            )

    @staticmethod
    def add_reports(reports: List[Report]) -> bool:
        with MariaDb() as mariadb:
            return mariadb.executemany(
                query="""
                INSERT INTO `sin_subsystems_reports` (
                    `subsystem_id`, `instant_record`,
                    `instant_load_following`
                ) VALUES (?, ?, ?)
                """,
                data=[report.serialize_data() for report in reports],
            )

    @staticmethod
    def is_empty_reports() -> bool:
        with MariaDb() as mariadb:
            cursor: Cursor = mariadb.execute(
                query="SELECT COUNT(`id`) FROM `sin_subsystems_reports`"
            )
            cursor_results: Tuple[int, ...] = cursor.fetchone()
            (count_reports,) = cursor_results
            return count_reports == 0

    @staticmethod
    def fetch_report(instant_record: str) -> Report | None:
        with MariaDb() as mariadb:
            cursor: Cursor = mariadb.execute(
                query="SELECT * FROM `sin_subsystems_reports` WHERE `instant_record`='?'",
                instant_record=instant_record,
            )
            cursor_results: Tuple[str, datetime, float] = cursor.fetchone()
            return Report(*cursor_results) if cursor_results.__len__() > 0 else None

    @staticmethod
    def fetch_distinct_instant_record_years() -> List[int]:
        if MariaDbUtils.is_empty_reports():
            yield None

        with MariaDb() as mariadb:
            current_year: int = brt_now().year
            cursor: Cursor = mariadb.execute(
                query="SELECT DISTINCT YEAR(`instant_record`) FROM `sin_subsystems_reports`"
            )
            for (year,) in cursor:
                if year < current_year:
                    yield year

    @staticmethod
    def fetch_latest_instant_record() -> datetime:
        if MariaDbUtils.is_empty_reports():
            return datetime.min

        with MariaDb() as mariadb:
            cursor: Cursor = mariadb.execute(
                query="SELECT MAX(`instant_record`) FROM `sin_subsystems_reports`"
            )
            cursor_results: Tuple[datetime, ...] = cursor.fetchone()
            if cursor_results.__len__() == 0:
                return datetime.min

            (instant_record,) = cursor_results
            return instant_record


class MariaDb(object):
    def __init__(self):
        self.__db_connection: Connection = None

    def __enter__(self):
        try:
            self.__db_connection: Connection = Connection(**Settings.CONFIG["database"])
        except Exception as err:
            warning(f"Error connecting to MariaDB Platform: {err}", RuntimeWarning)
        return self

    def __exit__(self, *args: Tuple[Any, ...]) -> None:
        self.__db_connection.close()

    def execute(self, query: str, data: Sequence = ()) -> Cursor | bool:
        is_select_statement: bool = bool(re.match(r"^(select).*", query.lower()))
        db_cursor: Cursor = self.__db_connection.cursor()
        try:
            db_cursor.execute(query.strip(), data)
            if is_select_statement:
                return db_cursor
            else:
                self.__db_connection.commit()
                return True
        except Exception as err:
            warning(
                f"Error while committing changes to database: {err}", RuntimeWarning
            )
            return False

    def executemany(self, query: str, data: List[Sequence]) -> bool:
        db_cursor: Cursor = self.__db_connection.cursor()
        try:
            db_cursor.executemany(query.strip(), data)
            return True
        except Exception as err:
            warning(
                f"Error while committing massive changes to database: {err}",
                RuntimeWarning,
            )
            return False
