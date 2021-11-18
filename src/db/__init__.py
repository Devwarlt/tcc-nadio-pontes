#!/usr/bin/env python3

from mariadb import *
from typing import *
from warnings import *
from datetime import *
from json import *


class Report(object):
    def __init__(self: Any, date: str, raw_data: bytes) -> Any:
        self.__date: datetime = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        self.__raw_data: Dict[str, Any] = loads(raw_data.decode('utf-8'))

    @property.getter
    def date(self: Any) -> datetime:
        return self.__date

    @property.setter
    def date(self: Any, date: datetime) -> datetime:
        self.__date = date

    @property.getter
    def raw_data(self: Any) -> Dict[str, Any]:
        return self.__raw_data

    @property.setter
    def raw_data(self: Any, raw_data: Dict[str, Any]) -> None:
        self.__raw_data = raw_data

    def raw_data_to_byte_array(self: Any) -> bytes:
        return dumps(raw_data).encode('utf-8')


class MariaDBUtils(object):
    def __init__(self: Any, *args, **kwargs) -> Any:
        raise SyntaxError("This is an utility class.")

    @staticmethod
    def store_new_report(report: Report) -> None:
        with MariaDB() as mariadb:
            mariadb.execute(
                query="INSERT INTO `report` (`date`, `raw_data`) VALUES (?, ?)",
                data=(report.date, report.raw_data_to_byte_array())
            )

    @staticmethod
    def fetch_report(date: str) -> Report:
        with MariaDB() as mariadb:
            return mariadb.execute(
                query="SELECT `date`, `raw_data` FROM `report` WHERE `date`=?",
                data=(date,),
                is_select_statement=True
            )

    @staticmethod
    def remove_report(date: str) -> Report:
        with MariaDB() as mariadb:
            mariadb.execute(
                query="DELETE FROM `report` WHERE `date`=?",
                data=(date,)
            )


class MariaDB(object):
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
    ) -> Any:
        db_cursor: Any = self.__db_connection.cursor()
        try:
            db_cursor.execute(query, data)
        except Error as err:
            warn(
                f"Error while committing changes to database: {err}",
                RuntimeWarning
            )

        if is_select_statement:
            return db_cursor
        else:
            self.__db_connection.commit()
