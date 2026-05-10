from contextlib import asynccontextmanager
import logging
import time

from fastapi import FastAPI, HTTPException, Request, Response, status
import uvicorn

from warehouse.events import WarehouseEvent
from warehouse.kafka_client import KafkaEventProducer, ensure_topics, kafka_is_available
from warehouse.logging_config import configure_logging
from warehouse.schema_registry import register_warehouse_schema
from warehouse.settings import Settings, get_settings
from warehouse.validation import EventValidationError, validate_event

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()
    retry_producer_setup(settings)
    producer = KafkaEventProducer(settings)
    app.state.settings = settings
    app.state.producer = producer
    logger.info("producer api started topic=%s", settings.warehouse_events_topic)
    try:
        yield
    finally:
        producer.close()


app = FastAPI(
    title="Smart Warehouse WMS Producer",
    version="0.1.0",
    lifespan=lifespan,
)


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
def publish_event(event: WarehouseEvent, request: Request, validate_payload: bool = True) -> dict:
    try:
        if validate_payload:
            validate_event(event)
        result = request.app.state.producer.publish(event)
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.reason) from exc
    except Exception as exc:
        logger.exception("failed to publish event")
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "event_id": event.event_id,
        "event_type": str(event.event_type),
        "topic": result.topic,
        "partition": result.partition,
        "offset": result.offset,
    }


@app.post("/events/batch", status_code=status.HTTP_202_ACCEPTED)
def publish_events(events: list[WarehouseEvent], request: Request) -> dict:
    published = []
    for event in events:
        try:
            validate_event(event)
            result = request.app.state.producer.publish(event)
            published.append(
                {
                    "event_id": event.event_id,
                    "event_type": str(event.event_type),
                    "topic": result.topic,
                    "partition": result.partition,
                    "offset": result.offset,
                }
            )
        except EventValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.reason) from exc
    return {"published": published}


@app.get("/health")
def health(request: Request, response: Response) -> dict:
    settings = request.app.state.settings
    kafka_ok = True
    last_error = None
    try:
        kafka_is_available(settings)
    except Exception as exc:
        kafka_ok = False
        last_error = str(exc)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "status": "ok" if kafka_ok else "unavailable",
        "kafka": kafka_ok,
        "kafka_bootstrap_servers": settings.kafka_bootstrap_servers,
        "schema_registry_url": settings.schema_registry_url,
        "last_error": last_error,
    }


def retry_producer_setup(settings: Settings) -> None:
    deadline = time.monotonic() + 120
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            ensure_topics(settings)
            register_warehouse_schema(settings)
            return
        except Exception as exc:
            last_error = exc
            logger.info("waiting for kafka/schema-registry: %s", exc)
            time.sleep(3)
    raise RuntimeError("Kafka or Schema Registry did not become available in time") from last_error


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "warehouse.producer_app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
