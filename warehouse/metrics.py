from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    disable_created_metrics,
    generate_latest,
)

disable_created_metrics()

REGISTRY = CollectorRegistry(auto_describe=True)

CONSUMER_LAG = Gauge(
    "consumer_lag",
    "Difference between latest Kafka offset and committed consumer offset.",
    ["topic", "partition"],
    registry=REGISTRY,
)

EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Number of warehouse events successfully processed.",
    ["event_type", "status"],
    registry=REGISTRY,
)

EVENT_PROCESSING_DURATION_SECONDS = Histogram(
    "event_processing_duration_seconds",
    "Warehouse event processing duration in seconds.",
    ["event_type"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
    registry=REGISTRY,
)

CASSANDRA_WRITE_ERRORS_TOTAL = Counter(
    "cassandra_write_errors_total",
    "Number of Cassandra write errors.",
    registry=REGISTRY,
)


def metrics_response() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
