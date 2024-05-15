from datetime import datetime, timedelta
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from forecast import IncidentForesight
from settings import Settings

root_router: APIRouter = APIRouter()

# FastAPI - Declare Request Example Data:
# https://fastapi.tiangolo.com/tutorial/schema-extra-example/

# Carga e Geração - ONS
# https://www.ons.org.br/paginas/energia-agora/carga-e-geracao


@root_router.get(
    "/incident_foresight",
    description="""
        Retrives all incident foresight predictions between 'start_period (required)' and 'final_period'
        (default: if not set, then API will calculate next upcoming 7 days since 'start_period' as
        reference).
        """,
    response_class=JSONResponse,
)
def get_incident_foresight_callback(
    start_period: datetime = datetime.today().date(),
    final_period: datetime | None = None,
) -> JSONResponse:
    if not final_period or final_period <= start_period:
        final_period = start_period + timedelta(days=7)

    incident_foresight: IncidentForesight = IncidentForesight(
        start_period, final_period, **Settings.CONFIG["forecast"]
    )
    incident_foresight.predict()
    return JSONResponse(list(incident_foresight.serialize_forecasts()))