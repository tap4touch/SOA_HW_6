import argparse
from datetime import UTC, datetime
from uuid import uuid4

import httpx

PRODUCER_URL = "http://127.0.0.1:8000"


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Smart Warehouse demo events.")
    parser.add_argument(
        "scenario",
        choices=["basic", "idempotency", "consistency", "out-of-order", "dlq", "cluster", "all"],
    )
    parser.add_argument("--producer-url", default=PRODUCER_URL)
    parser.add_argument("--prefix")
    args = parser.parse_args()

    scenarios = {
        "basic": send_basic,
        "idempotency": send_idempotency,
        "consistency": send_consistency,
        "out-of-order": send_out_of_order,
        "dlq": send_dlq,
        "cluster": send_cluster,
    }

    prefix = args.prefix
    if prefix is None:
        prefix = ""
        if args.scenario == "all":
            prefix = f"RUN-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-"

    if prefix:
        print({"prefix": prefix})

    if args.scenario == "all":
        for scenario in scenarios.values():
            scenario(args.producer_url, prefix)
    else:
        scenarios[args.scenario](args.producer_url, prefix)


def send_basic(producer_url: str, prefix: str = "") -> None:
    product_id = prefixed(prefix, "SKU-001")
    order_id = prefixed(prefix, "ORDER-001")
    post(
        producer_url,
        event("PRODUCT_RECEIVED", 1, product_id=product_id, zone_id="ZONE-A", quantity=100),
    )
    post(
        producer_url,
        event("PRODUCT_RESERVED", 2, product_id=product_id, zone_id="ZONE-A", quantity=30),
    )
    post(
        producer_url,
        event(
            "PRODUCT_MOVED",
            3,
            product_id=product_id,
            from_zone_id="ZONE-A",
            to_zone_id="ZONE-B",
            quantity=20,
        ),
    )
    post(
        producer_url,
        event("PRODUCT_SHIPPED", 4, product_id=product_id, zone_id="ZONE-A", quantity=10),
    )
    post(
        producer_url,
        event(
            "ORDER_CREATED",
            1,
            entity_key=f"order:{order_id}",
            product_id=product_id,
            order_id=order_id,
            items=[{"product_id": product_id, "zone_id": "ZONE-A", "quantity": 15}],
        ),
    )
    post(
        producer_url,
        event(
            "ORDER_COMPLETED",
            2,
            entity_key=f"order:{order_id}",
            product_id=product_id,
            order_id=order_id,
        ),
    )


def send_idempotency(producer_url: str, prefix: str = "") -> None:
    event_id = str(uuid4())
    product_id = prefixed(prefix, "SKU-002")
    payload = event(
        "PRODUCT_RECEIVED",
        1,
        event_id=event_id,
        product_id=product_id,
        zone_id="ZONE-A",
        quantity=50,
    )
    post(producer_url, payload)
    post(producer_url, payload)


def send_consistency(producer_url: str, prefix: str = "") -> None:
    post(
        producer_url,
        event(
            "PRODUCT_RECEIVED",
            1,
            product_id=prefixed(prefix, "SKU-003"),
            zone_id="ZONE-A",
            quantity=100,
        ),
    )


def send_out_of_order(producer_url: str, prefix: str = "") -> None:
    product_id = prefixed(prefix, "SKU-004")
    post(
        producer_url,
        event("PRODUCT_RECEIVED", 1, product_id=product_id, zone_id="ZONE-A", quantity=100),
    )
    post(
        producer_url,
        event("PRODUCT_SHIPPED", 2, product_id=product_id, zone_id="ZONE-A", quantity=20),
    )
    post(
        producer_url,
        event("PRODUCT_RECEIVED", 1, product_id=product_id, zone_id="ZONE-A", quantity=50),
    )


def send_dlq(producer_url: str, prefix: str = "") -> None:
    product_id = prefixed(prefix, "SKU-DLQ")
    post(
        producer_url,
        event("PRODUCT_SHIPPED", 1, product_id=product_id, zone_id="ZONE-A", quantity=-5),
        validate_payload=False,
    )
    post(
        producer_url,
        event("PRODUCT_RECEIVED", 2, product_id=product_id, zone_id="ZONE-A", quantity=10),
    )


def send_cluster(producer_url: str, prefix: str = "") -> None:
    product_id = prefixed(prefix, "SKU-006")
    post(
        producer_url,
        event("PRODUCT_RECEIVED", 1, product_id=product_id, zone_id="ZONE-A", quantity=200),
    )
    post(
        producer_url,
        event("PRODUCT_SHIPPED", 2, product_id=product_id, zone_id="ZONE-A", quantity=50),
    )


def prefixed(prefix: str, value: str) -> str:
    return f"{prefix}{value}" if prefix else value


def event(event_type: str, sequence_number: int, **fields) -> dict:
    payload = {
        "event_id": fields.pop("event_id", str(uuid4())),
        "event_type": event_type,
        "event_time": datetime.now(UTC).isoformat(),
        "sequence_number": sequence_number,
        "entity_key": fields.pop("entity_key", None),
        "product_id": None,
        "zone_id": None,
        "from_zone_id": None,
        "to_zone_id": None,
        "quantity": None,
        "counted_quantity": None,
        "order_id": None,
        "items": [],
    }
    payload.update(fields)
    return payload


def post(producer_url: str, payload: dict, validate_payload: bool = True) -> None:
    response = httpx.post(
        f"{producer_url}/events",
        params={"validate_payload": str(validate_payload).lower()},
        json=payload,
        timeout=10,
        trust_env=False,
    )
    if response.is_error:
        print(response.status_code)
        print(response.text)
    response.raise_for_status()
    print(response.json())


if __name__ == "__main__":
    main()
