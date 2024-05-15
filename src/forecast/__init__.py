from datetime import datetime
from typing import Any, Dict, Iterable, List
from prophet import Prophet
from pandas import DataFrame
from db import MariaDbUtils, Report, Subsystem
from utils import DATETIME_FORMAT, number_of_days_between


class IncidentForesight(object):
    def __init__(
        self, start_date: datetime, final_date: datetime, **prophet_args: Dict[str, Any]
    ) -> None:
        self.__model_args: Dict[str, Any] = prophet_args
        self.__start_date: datetime = start_date
        self.__final_date: datetime = final_date
        self.__subsystems_forecasts: List[
            Dict[str, str | Prophet | DataFrame | int]
        ] = []

    def predict(self) -> None:
        subsystems: List[Subsystem] = list(MariaDbUtils.fetch_subsystems())
        for subsystem in subsystems:
            subsystem_forecast: Dict[str, str | Prophet | DataFrame | int] = {
                "id": subsystem.subsystem_id,
                "name": subsystem.subsystem_name,
                "model": Prophet(**self.__model_args),
            }
            subsystem_reports: List[Report] = list(
                MariaDbUtils.fetch_reports_by_subsystem_id(subsystem.subsystem_id)
            )
            subsystem_forecast["input_data"] = DataFrame(
                [
                    subsystem_report.serialize_data()[1:]
                    for subsystem_report in subsystem_reports
                ],
                columns=["ds", "y"],
            )
            subsystem_forecast["model"].fit(subsystem_forecast["input_data"])
            subsystem_forecast["elapsed_days"] = number_of_days_between(
                self.__start_date, self.__final_date
            )
            foward_period: DataFrame = subsystem_forecast[
                "model"
            ].make_future_dataframe(subsystem_forecast["elapsed_days"])
            subsystem_forecast["forecast"] = subsystem_forecast["model"].predict(
                foward_period
            )
            self.__subsystems_forecasts.append(subsystem_forecast)

    def serialize_forecasts(
        self,
    ) -> Iterable[Dict[str, str | int | List[Dict[str, str | float]]]]:
        for subsystem_forecast in self.__subsystems_forecasts:
            yield {
                "id_subsistema": subsystem_forecast["id"],
                "nom_subsistema": subsystem_forecast["name"],
                "din_instante_inicio": self.__start_date.strftime(DATETIME_FORMAT),
                "din_instante_final": self.__final_date.strftime(DATETIME_FORMAT),
                "din_instante_periodo_dias": subsystem_forecast["elapsed_days"],
                "previsoes": list(
                    self.__serialize_subsystem_forecast(subsystem_forecast["forecast"])
                ),
            }

    def __serialize_subsystem_forecast(
        self, forecast: DataFrame
    ) -> Iterable[Dict[str, str | float]]:
        sliced_forecast: DataFrame = forecast[(forecast["ds"] >= self.__start_date)]
        sliced_forecast = sliced_forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
        for args in sliced_forecast.itertuples(index=False):
            instant_record: datetime = args[0]
            instant_load_following_estimated: float = args[1]
            instant_load_following_min: float = args[2]
            instant_load_following_max: float = args[3]
            yield {
                "din_instante": instant_record.strftime(DATETIME_FORMAT),
                "val_cargaenergiamwmed_estimado": instant_load_following_estimated,
                "val_cargaenergiamwmed_min": instant_load_following_min,
                "val_cargaenergiamwmed_max": instant_load_following_max,
                "val_cargaenergia_variacao": instant_load_following_max
                - instant_load_following_min,
            }