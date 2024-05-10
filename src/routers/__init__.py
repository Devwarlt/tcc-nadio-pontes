from datetime import datetime, timedelta
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from mock import MockUtils

root_router: APIRouter = APIRouter()

# FastAPI - Declare Request Example Data:
# https://fastapi.tiangolo.com/tutorial/schema-extra-example/


@root_router.get(
    "/incident_foresight",
    description="""
        Retrives all incident foresight predictions between 'start_period (required)' and 'end_period'
        (default: if not set, then API will calculate next upcoming 7 days since 'start_period' as
        reference).
        """,
    response_class=JSONResponse,
)
def get_incident_foresight_callback(
    start_period: datetime = datetime.today().date(),
    end_period: datetime | None = None,
) -> JSONResponse:
    if not end_period:
        end_period = start_period + timedelta(days=7)

    return JSONResponse(
        MockUtils.get_random_incident_foresight_results(start_period, end_period)
    )