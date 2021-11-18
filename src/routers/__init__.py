#!/usr/bin/env python3

from fastapi import *
from fastapi.responses import *
from typing import *
from db import *

router: APIRouter = APIRouter(
    responses={
        404: {"description": "Not found!"}
    }
)

@router.get("/report", response_class=JSONResponse)
def get_report_callback(date: str) -> JSONResponse:
    report: Report = MariaDBUtils.fetch_report(date)
    json_reponse: JSONResponse = JSONResponse(report)