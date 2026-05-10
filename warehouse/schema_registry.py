import logging
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from warehouse.settings import Settings

logger = logging.getLogger(__name__)


def load_schema(schema_path: Path) -> str:
    return schema_path.read_text(encoding="utf-8")


def schema_subject(settings: Settings) -> str:
    return f"{settings.warehouse_events_topic}-value"


def create_schema_registry_client(settings: Settings) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": settings.schema_registry_url})


def register_warehouse_schema(settings: Settings) -> int:
    schema_str = load_schema(settings.schema_path)
    client = create_schema_registry_client(settings)
    schema = Schema(schema_str, "AVRO")
    subject = schema_subject(settings)
    schema_id = client.register_schema(subject, schema)
    logger.info("registered schema subject=%s schema_id=%s", subject, schema_id)
    return schema_id
