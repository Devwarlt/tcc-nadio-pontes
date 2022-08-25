from mariadb import *
from typing import *
from warnings import *
from datetime import *
from json import *


class Report(object):
    __DATETIME_FORMAT_PATTERN: str = "%Y-%m-%d %H:%M:%S"
    __STRING_ENCODER_PATTERN: str = "utf-8"


    def __init__(self: Any, date: str, raw_data: Union[bytes, str]) -> Any:
        self.__date: datetime = datetime.strptime(date, self.__DATETIME_FORMAT_PATTERN)
        self.__raw_data: Dict[str, Any] = None
        if isinstance(raw_data, bytes):
            self.__raw_data = loads(raw_data.decode(self.__STRING_ENCODER_PATTERN))
        else:
            self.__raw_data = loads(raw_data)

    @property
    def date(self: Any) -> datetime:
        return self.__date

    @date.setter
    def date(self: Any, value: datetime) -> datetime:
        self.__date = value

    @property
    def raw_data(self: Any) -> Dict[str, Any]:
        return self.__raw_data

    @raw_data.setter
    def raw_data(self: Any, value: Dict[str, Any]) -> None:
        self.__raw_data = value

    def serialize_data(self: Any) -> Tuple[str, bytes,]:
        date_str: str = self.__date.strftime(self.__DATETIME_FORMAT_PATTERN)
        raw_data_byte_array: bytes = dumps(self.raw_data)\
            .encode(self.__STRING_ENCODER_PATTERN)
        return (date_str, raw_data_byte_array,)

    def to_json(self: Any) -> Dict[str, Any]:
        return {
            'date': self.__date.strftime(self.__DATETIME_FORMAT_PATTERN),
            'raw_data': self.__raw_data
        }


class MariaDbUtils(object):
    def __init__(self: Any, *args, **kwargs) -> Any:
        raise SyntaxError("This is an utility class.")

    @staticmethod
    def store_new_report(report: Report) -> Literal[0, 1]:
        with MariaDb() as mariadb:
            return mariadb.execute(
                query="INSERT INTO `report` (`date`, `raw_data`) VALUES (?, ?)",
                data=report.serialize_data()
            )

    @staticmethod
    def fetch_report(date: str) -> Report:
        with MariaDb() as mariadb:
            report_data =  mariadb.execute(
                query="SELECT `date`, `raw_data` FROM `report` WHERE `date`=?",
                data=(date,),
                is_select_statement=True
            )
            fetched_item: Tuple[Any, ...] = report_data.fetchone()
            if fetched_item:
                # print(*fetched_item, sep=" ")
                _, raw_data = fetched_item
                report = Report(date, raw_data)
                return report
            else:
                return None

    @staticmethod
    def remove_report(date: str) -> Literal[0, 1]:
        with MariaDb() as mariadb:
            return mariadb.execute(
                query="DELETE FROM `report` WHERE `date`=?",
                data=(date,)
            )


class MariaDb(object):
    def __init__(self: Any) -> Any:
        self.__db_params: Dict[str, Any] = {
            'user': "root",
            'password': "toor",
            'host': "localhost",
            'database': "sisbin"
        }
        self.__db_connection: connection = None

    def __enter__(self: Any) -> Any:
        try:
            self.__db_connection: connection = connection(**self.__db_params)
        except Error as err:
            warn(
                f"Error connecting to MariaDB Platform: {err}",
                RuntimeWarning
            )
        return self

    def __exit__(self: Any, type: Any, value: Any, statement: Any) -> None:
        self.__db_connection.close()

    def execute(
        self: Any, query: str, data: Tuple[Any, ...], is_select_statement: bool = False
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
            warn(
                f"Error while committing changes to database: {err}",
                RuntimeWarning
            )
            if is_select_statement:
                return None
            else:
                return 1
