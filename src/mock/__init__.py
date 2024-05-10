from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from random import randint, random
from utils import DATETIME_FORMAT


import math


class MockUtils(object):
    def __init__(self, *args: Tuple[Any, ...]) -> None:
        raise SyntaxError("This is an utility class.")

    @staticmethod
    def get_random_incident_foresight_results(
        time_start: datetime, time_end: datetime
    ) -> List[Dict[str, datetime | float]]:
        incident_foresight_results: List[Dict[str, datetime | float]] = []
        total_entries: int = (time_end - time_start).days
        for i in range(total_entries + 1):
            incident_foresight_results.append(
                {
                    "date": (time_start + timedelta(days=i)).strftime(DATETIME_FORMAT),
                    "load_value_prediction": round(
                        random() * math.pow(10, randint(3, 5)), 2
                    ),
                }
            )
        return incident_foresight_results