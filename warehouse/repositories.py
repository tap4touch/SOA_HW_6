from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import Any

from cassandra import ConsistencyLevel  # type: ignore[attr-defined]
from cassandra.query import BatchStatement, BatchType, SimpleStatement

from warehouse.events import OrderItem, WarehouseEvent
from warehouse.metrics import CASSANDRA_WRITE_ERRORS_TOTAL
from warehouse.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class KafkaMetadata:
    partition: int
    offset: int


@dataclass(slots=True)
class InventoryState:
    product_id: str
    zone_id: str
    available_quantity: int = 0
    reserved_quantity: int = 0


@dataclass(slots=True)
class InventoryTotal:
    product_id: str
    total_available_quantity: int = 0
    total_reserved_quantity: int = 0


@dataclass(slots=True)
class OrderRecord:
    order_id: str
    status: str
    items: list[OrderItem]
    created_at: datetime | None


class WarehouseRepository:
    def __init__(self, session: Any, settings: Settings) -> None:
        self.session = session
        self.settings = settings
        self.read_consistency = consistency_from_name(settings.cassandra_read_consistency)
        self.write_consistency = consistency_from_name(settings.cassandra_write_consistency)

    def ping(self) -> bool:
        statement = SimpleStatement(
            "SELECT product_id FROM inventory_by_product LIMIT 1",
            consistency_level=self.read_consistency,
        )
        self.session.execute(statement).one()
        return True

    def new_batch(self) -> BatchStatement:
        return BatchStatement(
            batch_type=BatchType.LOGGED,
            consistency_level=self.write_consistency,
        )

    def execute_batch(self, batch: BatchStatement) -> None:
        try:
            self.session.execute(batch)
        except Exception:
            CASSANDRA_WRITE_ERRORS_TOTAL.inc()
            raise

    def is_processed(self, event_id: str) -> bool:
        statement = SimpleStatement(
            "SELECT event_id FROM processed_events WHERE event_id = %s",
            consistency_level=self.read_consistency,
        )
        row = self.session.execute(statement, (event_id,)).one()
        return row is not None

    def get_last_sequence(self, entity_key: str) -> int | None:
        statement = SimpleStatement(
            "SELECT last_sequence_number FROM entity_versions WHERE entity_key = %s",
            consistency_level=self.read_consistency,
        )
        row = self.session.execute(statement, (entity_key,)).one()
        return row.last_sequence_number if row else None

    def get_inventory(self, product_id: str, zone_id: str) -> InventoryState:
        statement = SimpleStatement(
            """
            SELECT available_quantity, reserved_quantity
            FROM inventory_by_product_zone
            WHERE product_id = %s AND zone_id = %s
            """,
            consistency_level=self.read_consistency,
        )
        row = self.session.execute(statement, (product_id, zone_id)).one()
        if row is None:
            return InventoryState(product_id=product_id, zone_id=zone_id)
        return InventoryState(
            product_id=product_id,
            zone_id=zone_id,
            available_quantity=row.available_quantity or 0,
            reserved_quantity=row.reserved_quantity or 0,
        )

    def get_inventory_total(self, product_id: str) -> InventoryTotal:
        statement = SimpleStatement(
            """
            SELECT total_available_quantity, total_reserved_quantity
            FROM inventory_by_product
            WHERE product_id = %s
            """,
            consistency_level=self.read_consistency,
        )
        row = self.session.execute(statement, (product_id,)).one()
        if row is None:
            return InventoryTotal(product_id=product_id)
        return InventoryTotal(
            product_id=product_id,
            total_available_quantity=row.total_available_quantity or 0,
            total_reserved_quantity=row.total_reserved_quantity or 0,
        )

    def get_order(self, order_id: str) -> OrderRecord | None:
        statement = SimpleStatement(
            "SELECT order_id, status, items_json, created_at FROM orders_by_id WHERE order_id = %s",
            consistency_level=self.read_consistency,
        )
        row = self.session.execute(statement, (order_id,)).one()
        if row is None:
            return None

        raw_items = json.loads(row.items_json)
        return OrderRecord(
            order_id=row.order_id,
            status=row.status,
            items=[OrderItem.model_validate(item) for item in raw_items],
            created_at=row.created_at,
        )

    def add_inventory_update(
        self,
        batch: BatchStatement,
        state: InventoryState,
        event: WarehouseEvent,
        updated_at: datetime,
    ) -> None:
        params = (
            state.available_quantity,
            state.reserved_quantity,
            event.event_id,
            event.sequence_number,
            updated_at,
            state.product_id,
            state.zone_id,
        )
        batch.add(
            """
            UPDATE inventory_by_product_zone
            SET available_quantity = %s,
                reserved_quantity = %s,
                last_event_id = %s,
                last_sequence_number = %s,
                updated_at = %s
            WHERE product_id = %s AND zone_id = %s
            """,
            params,
        )
        batch.add(
            """
            UPDATE inventory_by_zone
            SET available_quantity = %s,
                reserved_quantity = %s,
                last_event_id = %s,
                last_sequence_number = %s,
                updated_at = %s
            WHERE zone_id = %s AND product_id = %s
            """,
            (
                state.available_quantity,
                state.reserved_quantity,
                event.event_id,
                event.sequence_number,
                updated_at,
                state.zone_id,
                state.product_id,
            ),
        )

    def add_total_update(
        self,
        batch: BatchStatement,
        total: InventoryTotal,
        event: WarehouseEvent,
        updated_at: datetime,
    ) -> None:
        batch.add(
            """
            UPDATE inventory_by_product
            SET total_available_quantity = %s,
                total_reserved_quantity = %s,
                last_event_id = %s,
                last_sequence_number = %s,
                updated_at = %s
            WHERE product_id = %s
            """,
            (
                total.total_available_quantity,
                total.total_reserved_quantity,
                event.event_id,
                event.sequence_number,
                updated_at,
                total.product_id,
            ),
        )

    def add_order_update(
        self,
        batch: BatchStatement,
        event: WarehouseEvent,
        status: str,
        items: list[OrderItem],
        created_at: datetime | None,
        completed_at: datetime | None,
        updated_at: datetime,
    ) -> None:
        batch.add(
            """
            UPDATE orders_by_id
            SET status = %s,
                items_json = %s,
                created_at = %s,
                completed_at = %s,
                last_event_id = %s,
                last_sequence_number = %s
            WHERE order_id = %s
            """,
            (
                status,
                json.dumps([item.model_dump(mode="json") for item in items], ensure_ascii=False),
                created_at,
                completed_at,
                event.event_id,
                event.sequence_number,
                event.order_id,
            ),
        )
        logger.info(
            "prepared order update order_id=%s status=%s at=%s", event.order_id, status, updated_at
        )

    def add_processing_records(
        self,
        batch: BatchStatement,
        event: WarehouseEvent,
        metadata: KafkaMetadata,
        status: str,
        processed_at: datetime,
        error_reason: str | None = None,
        update_version: bool = True,
    ) -> None:
        entity_key = event.resolved_entity_key()
        batch.add(
            """
            INSERT INTO processed_events (
                event_id,
                event_type,
                entity_key,
                sequence_number,
                processing_status,
                processed_at,
                kafka_partition,
                kafka_offset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.event_id,
                str(event.event_type),
                entity_key,
                event.sequence_number,
                status,
                processed_at,
                metadata.partition,
                metadata.offset,
            ),
        )
        batch.add(
            """
            INSERT INTO event_history_by_product (
                product_id,
                event_time,
                event_id,
                event_type,
                entity_key,
                sequence_number,
                processing_status,
                payload,
                error_reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.history_product_id(),
                event.event_time,
                event.event_id,
                str(event.event_type),
                entity_key,
                event.sequence_number,
                status,
                json.dumps(event.to_avro_dict(), ensure_ascii=False),
                error_reason,
            ),
        )
        if update_version:
            batch.add(
                """
                UPDATE entity_versions
                SET last_sequence_number = %s,
                    last_event_id = %s,
                    updated_at = %s
                WHERE entity_key = %s
                """,
                (event.sequence_number, event.event_id, processed_at, entity_key),
            )


def consistency_from_name(name: str) -> int:
    normalized = name.upper()
    if not hasattr(ConsistencyLevel, normalized):
        raise ValueError(f"Unknown Cassandra consistency level: {name}")
    return getattr(ConsistencyLevel, normalized)
