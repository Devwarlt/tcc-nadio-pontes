from fastapi import *
from fastapi.responses import *
from typing import *
from db import *

root_router: APIRouter = APIRouter(
    responses={
        404: {"description": "Not found!"}
    }
)

@root_router.get("/report", response_class=JSONResponse)
def get_report_callback(date: str) -> JSONResponse:
    report: Report = MariaDbUtils.fetch_report(date)
    json_reponse: JSONResponse\
        = JSONResponse(
            report.to_json() if report\
            else {'data': "null"}
        )
    return json_reponse

@root_router.post("/report", response_class=JSONResponse)
def post_report_callback(date: str, raw_data: str) -> JSONResponse:
    report: Report = Report(date, raw_data)
    exit_code: Literal[0, 1] = MariaDbUtils.store_new_report(report)
    json_reponse: JSONResponse = JSONResponse({"exit_code": exit_code})
    return json_reponse

@root_router.delete("/report", response_class=JSONResponse)
def delete_report_callback(date: str) -> JSONResponse:
    exit_code: Literal[0, 1] = MariaDbUtils.remove_report(date)
    json_reponse: JSONResponse = JSONResponse({"exit_code": exit_code})
    return json_reponse

@root_router.get("/incident_foresight", response_class=JSONResponse)
def get_incident_foresight_callback(date: str) -> JSONResponse:
    json_reponse: JSONResponse = JSONResponse({"exit_code": "TODO!"})
    return json_reponse

@root_router.get("/populate", response_class=JSONResponse)
def get_populate_callback(date: str) -> JSONResponse:
    json_reponse: JSONResponse = JSONResponse({"exit_code": "TODO!"})
    return json_reponse