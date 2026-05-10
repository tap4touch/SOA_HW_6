import argparse
import json
import time
from uuid import uuid4

from confluent_kafka import Consumer

from warehouse.settings import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Read messages from warehouse-events-dlq.")
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--group-id")
    args = parser.parse_args()

    settings = get_settings()
    group_id = args.group_id or f"warehouse-dlq-reader-{uuid4()}"
    consumer = Consumer(
        {
            "bootstrap.servers": local_bootstrap_servers(settings.kafka_bootstrap_servers),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.warehouse_dlq_topic])
    deadline = time.monotonic() + args.seconds
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                print(message.error())
                continue
            value = message.value()
            if value is None:
                continue
            print(json.dumps(json.loads(value), ensure_ascii=False, indent=2))
    finally:
        consumer.close()


def local_bootstrap_servers(bootstrap_servers: str) -> str:
    return bootstrap_servers.replace("localhost:", "127.0.0.1:")


if __name__ == "__main__":
    main()
