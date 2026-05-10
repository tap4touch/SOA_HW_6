import logging
from dataclasses import dataclass
from threading import Event

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from warehouse.events import WarehouseEvent
from warehouse.schema_registry import create_schema_registry_client, load_schema
from warehouse.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PublishResult:
    topic: str
    partition: int
    offset: int


def ensure_topics(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = [
        NewTopic(
            settings.warehouse_events_topic,
            num_partitions=settings.kafka_partitions,
            replication_factor=settings.kafka_replication_factor,
        ),
        NewTopic(
            settings.warehouse_dlq_topic,
            num_partitions=settings.kafka_partitions,
            replication_factor=settings.kafka_replication_factor,
        ),
    ]

    futures = admin.create_topics(topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("created kafka topic=%s", topic_name)
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info("kafka topic already exists topic=%s", topic_name)
                continue
            raise


def kafka_is_available(settings: Settings) -> bool:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    admin.list_topics(timeout=2)
    return True


class KafkaEventProducer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        schema_registry_client = create_schema_registry_client(settings)
        schema_str = load_schema(settings.schema_path)
        self.value_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            lambda event, _: event.to_avro_dict(),
        )
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    def publish(self, event: WarehouseEvent) -> PublishResult:
        delivered = Event()
        result: dict[str, PublishResult] = {}
        failure: dict[str, Exception] = {}
        context = SerializationContext(self.settings.warehouse_events_topic, MessageField.VALUE)
        value = self.value_serializer(event, context)
        key = event.topic_key().encode("utf-8")

        def on_delivery(error, message) -> None:
            if error is not None:
                failure["error"] = KafkaException(error)
            else:
                result["value"] = PublishResult(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
            delivered.set()

        self.producer.produce(
            self.settings.warehouse_events_topic,
            key=key,
            value=value,
            on_delivery=on_delivery,
        )
        self.producer.flush(10)
        delivered.wait(10)
        if failure:
            raise failure["error"]
        if "value" not in result:
            raise TimeoutError("Kafka delivery callback did not finish in time")
        return result["value"]

    def close(self) -> None:
        self.producer.flush(5)


def create_consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.warehouse_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )


def create_avro_deserializer(settings: Settings) -> AvroDeserializer:
    schema_registry_client = create_schema_registry_client(settings)
    schema_str = load_schema(settings.schema_path)
    return AvroDeserializer(schema_registry_client, schema_str)


def deserialize_event(
    deserializer: AvroDeserializer,
    topic: str,
    value: bytes | None,
) -> WarehouseEvent:
    context = SerializationContext(topic, MessageField.VALUE)
    payload = deserializer(value, context)
    if payload is None:
        raise ValueError("Kafka message value is empty")
    if not isinstance(payload, dict):
        raise ValueError(f"Kafka message value must be a record, got {type(payload).__name__}")
    return WarehouseEvent.from_avro_dict(payload)


def topic_partition_from_message(message) -> TopicPartition:
    return TopicPartition(message.topic(), message.partition(), message.offset() + 1)
