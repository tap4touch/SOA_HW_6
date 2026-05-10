from datetime import UTC, datetime
import json
import logging
import traceback

from confluent_kafka import KafkaException, Producer

from warehouse.events import WarehouseEvent
from warehouse.settings import Settings

logger = logging.getLogger(__name__)


class DeadLetterProducer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    def publish(
        self,
        event: WarehouseEvent | None,
        error_reason: str,
        error_code: str,
        partition: int,
        offset: int,
        include_traceback: bool = False,
    ) -> None:
        payload = {
            "original_event": event.to_avro_dict() if event else None,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": datetime.now(UTC).isoformat(),
            "kafka_metadata": {
                "partition": partition,
                "offset": offset,
            },
        }
        if include_traceback:
            payload["traceback"] = traceback.format_exc()

        delivery_error: list[KafkaException] = []

        def on_delivery(error, _) -> None:
            if error is not None:
                delivery_error.append(KafkaException(error))

        key = event.event_id.encode("utf-8") if event else b"unknown-event"
        self.producer.produce(
            self.settings.warehouse_dlq_topic,
            key=key,
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            on_delivery=on_delivery,
        )
        self.producer.flush(10)
        if delivery_error:
            raise delivery_error[0]
        logger.info(
            "sent event to dlq event_id=%s error_code=%s partition=%s offset=%s",
            event.event_id if event else None,
            error_code,
            partition,
            offset,
        )

    def close(self) -> None:
        self.producer.flush(5)
