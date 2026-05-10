import logging
from threading import Event, Thread
import time
from typing import Any

from confluent_kafka import KafkaException, TopicPartition

from warehouse.cassandra_client import connect_with_retry
from warehouse.cassandra_migrations import apply_migrations
from warehouse.dlq import DeadLetterProducer
from warehouse.event_handlers import BusinessRuleError, EventProcessor
from warehouse.events import WarehouseEvent
from warehouse.kafka_client import (
    create_avro_deserializer,
    create_consumer,
    deserialize_event,
    ensure_topics,
    kafka_is_available,
)
from warehouse.metrics import (
    CONSUMER_LAG,
    EVENT_PROCESSING_DURATION_SECONDS,
    EVENTS_PROCESSED_TOTAL,
)
from warehouse.repositories import KafkaMetadata, WarehouseRepository
from warehouse.schema_registry import register_warehouse_schema
from warehouse.settings import Settings
from warehouse.validation import EventValidationError

logger = logging.getLogger(__name__)


class WarehouseConsumerService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.stop_requested = Event()
        self.thread: Thread | None = None
        self.consumer: Any = None
        self.dlq: DeadLetterProducer | None = None
        self.repository: WarehouseRepository | None = None
        self.cluster: Any = None
        self.session: Any = None
        self.running = False
        self.last_error: str | None = None

    def start(self) -> None:
        retry_infrastructure_setup(self.settings)
        self.cluster, self.session = connect_with_retry(self.settings)
        apply_migrations(self.session, self.settings)
        self.repository = WarehouseRepository(self.session, self.settings)
        self.consumer = create_consumer(self.settings)
        self.dlq = DeadLetterProducer(self.settings)
        self.thread = Thread(target=self.run, name="warehouse-consumer-loop", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_requested.set()
        if self.thread:
            self.thread.join(timeout=15)
        if self.consumer:
            self.consumer.close()
        if self.dlq:
            self.dlq.close()
        if self.cluster:
            self.cluster.shutdown()

    def health(self) -> tuple[bool, dict[str, str | bool | None]]:
        kafka_ok = False
        if self.running and self.consumer is not None:
            try:
                kafka_ok = kafka_is_available(self.settings)
            except Exception as exc:
                self.last_error = str(exc)
        cassandra_ok = False
        if self.repository is not None:
            try:
                cassandra_ok = self.repository.ping()
            except Exception as exc:
                self.last_error = str(exc)
        ok = kafka_ok and cassandra_ok
        return ok, {
            "status": "ok" if ok else "unavailable",
            "kafka": kafka_ok,
            "cassandra": cassandra_ok,
            "last_error": self.last_error,
        }

    def run(self) -> None:
        assert self.consumer is not None
        assert self.repository is not None
        assert self.dlq is not None

        deserializer = create_avro_deserializer(self.settings)
        processor = EventProcessor(self.repository)
        self.consumer.subscribe([self.settings.warehouse_events_topic])
        self.running = True
        logger.info(
            "consumer started topic=%s group_id=%s",
            self.settings.warehouse_events_topic,
            self.settings.warehouse_consumer_group,
        )

        while not self.stop_requested.is_set():
            try:
                message = self.consumer.poll(1.0)
                if message is None:
                    self.update_lag()
                    continue
                if message.error():
                    self.last_error = str(message.error())
                    logger.error("kafka consumer error=%s", message.error())
                    continue

                self.handle_message(message, deserializer, processor)
                self.update_lag()
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("unexpected consumer loop error")
                time.sleep(1)

        self.running = False
        logger.info("consumer stopped")

    def handle_message(self, message, deserializer, processor: EventProcessor) -> None:
        assert self.consumer is not None
        assert self.dlq is not None

        event: WarehouseEvent | None = None
        metadata = KafkaMetadata(partition=message.partition(), offset=message.offset())

        try:
            event = deserialize_event(deserializer, message.topic(), message.value())
            with EVENT_PROCESSING_DURATION_SECONDS.labels(str(event.event_type)).time():
                status = processor.process(event, metadata)

            EVENTS_PROCESSED_TOTAL.labels(str(event.event_type), status).inc()
            self.consumer.commit(message=message, asynchronous=False)
            logger.info(
                "processed event_id=%s event_type=%s status=%s partition=%s offset=%s",
                event.event_id,
                event.event_type,
                status,
                message.partition(),
                message.offset(),
            )
        except EventValidationError as exc:
            self.send_to_dlq_and_commit(message, event, exc.reason, exc.error_code)
        except BusinessRuleError as exc:
            self.send_to_dlq_and_commit(message, event, exc.reason, exc.error_code)
        except Exception as exc:
            self.send_to_dlq_and_commit(
                message,
                event,
                str(exc),
                "PROCESSING_ERROR",
                include_traceback=True,
            )

    def send_to_dlq_and_commit(
        self,
        message,
        event: WarehouseEvent | None,
        reason: str,
        error_code: str,
        include_traceback: bool = False,
    ) -> None:
        assert self.consumer is not None
        assert self.dlq is not None

        self.dlq.publish(
            event=event,
            error_reason=reason,
            error_code=error_code,
            partition=message.partition(),
            offset=message.offset(),
            include_traceback=include_traceback,
        )
        self.consumer.commit(message=message, asynchronous=False)
        event_type = str(event.event_type) if event else "UNKNOWN"
        EVENTS_PROCESSED_TOTAL.labels(event_type, "DLQ").inc()
        logger.info(
            "committed dlq event_id=%s error_code=%s partition=%s offset=%s",
            event.event_id if event else None,
            error_code,
            message.partition(),
            message.offset(),
        )

    def update_lag(self) -> None:
        if self.consumer is None:
            return
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return
            committed = self.consumer.committed(assignment, timeout=5)
            for partition in committed:
                watermark_partition = TopicPartition(partition.topic, partition.partition)
                _, high = self.consumer.get_watermark_offsets(
                    watermark_partition,
                    timeout=5,
                    cached=False,
                )
                committed_offset = max(partition.offset, 0)
                lag = max(high - committed_offset, 0)
                CONSUMER_LAG.labels(partition.topic, str(partition.partition)).set(lag)
        except KafkaException as exc:
            self.last_error = str(exc)
            logger.warning("could not update consumer lag: %s", exc)


def retry_infrastructure_setup(settings: Settings) -> None:
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
