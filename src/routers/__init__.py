#!/usr/bin/env python3

from fastapi import *
from fastapi.responses import *
from typing import *
from mocks import *


router: APIRouter = APIRouter(
    responses={
        404: {"description": "Not found!"}
    }
)

@router.get("/report", response_class=JSONResponse)
def get_report_callback(date: str) -> JSONResponse:
    JSONResponse()