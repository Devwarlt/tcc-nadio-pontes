from db import *


def get_report(instant_record: str) -> Report:
    report: Report = MariaDbUtils.fetch_report(instant_record)
    return report