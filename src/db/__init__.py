from typing import *
from mariadb import *
from typing import *
from warnings import *
from datetime import *
from json import *
from settings import *


class Report(object):
    __DATETIME_FORMAT_PATTERN: str = "%Y-%m-%d %H:%M:%S"
    # __STRING_ENCODER_PATTERN: str = "utf-8"

    def __init__(
        self,
        subsystem_id: str,
        instant_record: datetime,
        instant_load_following: float,
    ):
        self.__subsystem_id: str = subsystem_id
        self.__instant_record: datetime = instant_record
        self.__instant_load_following: float = instant_load_following

        # try:
        #     self.__instant_record: datetime = datetime.strptime(date, self.__DATETIME_FORMAT_PATTERN)
        # except ValueError:
        #     self.__instant_record: datetime = datetime.strptime(f'{date} 00:00:00', self.__DATETIME_FORMAT_PATTERN)

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

    def serialize_data(self) -> Tuple[str, str, float]:
        subsystem_id: str = self.__subsystem_id
        instant_record: str = self.__instant_record.strftime(
            self.__DATETIME_FORMAT_PATTERN
        )
        instant_load_following: float = self.__instant_load_following
        return (
            subsystem_id,
            instant_record,
            instant_load_following,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "subsystem_id": self.__subsystem_id,
            "instant_record": self.__instant_record.strftime(
                self.__DATETIME_FORMAT_PATTERN
            ),
            "instant_load_following": self.__instant_load_following,
        }


class MariaDbUtils(object):
    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]):
        raise SyntaxError("This is an utility class.")

    @staticmethod
    def add_report(report: Report) -> Literal[0, 1]:
        with MariaDb() as mariadb:
            return mariadb.execute(
                query="INSERT INTO `sin_subsystems_reports` (\
                    `subsystem_id`, `instant_record`, \
                    `instant_load_following`) \
                    VALUES ('?', '?', ?)",
                data=report.serialize_data(),
            )

    @staticmethod
    def fetch_report(instant_record: str) -> Report:
        with MariaDb() as mariadb:
            report_data = mariadb.execute(
                query="SELECT * FROM `sin_subsystems_reports` WHERE `instant_record`='?'",
                instant_record=instant_record,
                is_select_statement=True,
            )
            fetched_item: Tuple[Any, ...] = report_data.fetchone()
            return Report(*fetched_item) if fetched_item else None


class MariaDb(object):
    def __init__(self):
        self.__db_connection: Connection = None

    def __enter__(self):
        try:
            self.__db_connection: Connection = Connection(**Settings.CONFIG["database"])
        except Error as err:
            warn(f"Error connecting to MariaDB Platform: {err}", RuntimeWarning)
        return self

    def __exit__(self, type, value, statement) -> None:
        self.__db_connection.close()

    def execute(
        self, query: str, data: Tuple[Any, ...], is_select_statement: bool = False
    ) -> Union[Any, Literal[0, 1]]:
        db_cursor: Any = self.__db_connection.cursor()
        try:
            db_cursor.execute(query, data)
            if is_select_statement:
                return db_cursor
            else:
                self.__db_connection.commit()
                return 0
        except Error as err:
            warn(f"Error while committing changes to database: {err}", RuntimeWarning)
            return None if is_select_statement else 1
