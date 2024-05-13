from datetime import datetime
from typing import Any, Dict, List
from prophet import Prophet
from pandas import DataFrame
from utils import number_of_days_between


class IncidentForesight(object):
    def __init__(
        self, start_date: datetime, final_date: datetime, **prophet_args: Dict[str, Any]
    ) -> None:
        self.__model: Prophet = Prophet(**prophet_args)
        self.__current_dataframe: DataFrame = None
        self.__future_dataframe: DataFrame = None
        self.__forecast_dataframe: DataFrame = None

        if start_date >= final_date:
            raise ValueError("'start_date' is equal and greater than 'final_date'")

        self.__start_date: datetime = start_date
        self.__final_date: datetime = final_date

    def fit(self, partial_report: DataFrame) -> None:
        self.__current_dataframe = partial_report.rename(
            columns={"instant_record": "ds", "instant_load_following": "y"}
        )
        self.__model.fit(self.__current_dataframe)

    def calculate_prediction(self) -> None:
        total_days: int = number_of_days_between(self.__start_date, self.__final_date)
        self.__future_dataframe = self.__model.make_future_dataframe(
            periods=total_days, freq="D"
        )
        self.__forecast_dataframe = self.__model.predict(self.__future_dataframe)

    def fetch_predictions(self) -> List[Dict[str, datetime | float]]:
        pass  # get rows between date difference