from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
import uvicorn

from warehouse.consumer_service import WarehouseConsumerService
from warehouse.logging_config import configure_logging
from warehouse.metrics import metrics_response
from warehouse.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()
    service = WarehouseConsumerService(settings)
    service.start()
    app.state.settings = settings
    app.state.consumer_service = service
    try:
        yield
    finally:
        service.stop()


app = FastAPI(
    title="Smart Warehouse Consumer",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health(request: Request, response: Response) -> dict:
    ok, payload = request.app.state.consumer_service.health()
    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return payload


@app.get("/metrics")
def metrics() -> Response:
    body, content_type = metrics_response()
    return Response(content=body, media_type=content_type)


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "warehouse.consumer_app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
