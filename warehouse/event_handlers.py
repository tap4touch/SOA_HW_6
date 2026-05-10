from datetime import UTC, datetime

from warehouse.events import EventType, WarehouseEvent
from warehouse.repositories import (
    InventoryState,
    InventoryTotal,
    KafkaMetadata,
    WarehouseRepository,
)
from warehouse.validation import EventValidationError, validate_event


class BusinessRuleError(Exception):
    def __init__(self, error_code: str, reason: str) -> None:
        super().__init__(reason)
        self.error_code = error_code
        self.reason = reason


class EventProcessor:
    def __init__(self, repository: WarehouseRepository) -> None:
        self.repository = repository

    def process(self, event: WarehouseEvent, metadata: KafkaMetadata) -> str:
        if self.repository.is_processed(event.event_id):
            return "DUPLICATE"

        validate_event(event)
        entity_key = event.resolved_entity_key()
        last_sequence = self.repository.get_last_sequence(entity_key)
        if last_sequence is not None and event.sequence_number <= last_sequence:
            self.record_ignored(event, metadata, last_sequence)
            return "IGNORED_OUT_OF_ORDER"

        batch = self.repository.new_batch()
        updated_at = datetime.now(UTC)
        inventory = InventoryAccumulator(self.repository, event, batch, updated_at)

        match EventType(event.event_type):
            case EventType.PRODUCT_RECEIVED:
                quantity = required_int(event.quantity, "quantity")
                inventory.change(event.product_id, event.zone_id, available_delta=quantity)
            case EventType.PRODUCT_SHIPPED:
                quantity = required_int(event.quantity, "quantity")
                inventory.change(event.product_id, event.zone_id, available_delta=-quantity)
            case EventType.PRODUCT_MOVED:
                quantity = required_int(event.quantity, "quantity")
                inventory.change(event.product_id, event.from_zone_id, available_delta=-quantity)
                inventory.change(event.product_id, event.to_zone_id, available_delta=quantity)
            case EventType.PRODUCT_RESERVED:
                quantity = required_int(event.quantity, "quantity")
                inventory.change(
                    event.product_id,
                    event.zone_id,
                    available_delta=-quantity,
                    reserved_delta=quantity,
                )
            case EventType.PRODUCT_RELEASED:
                quantity = required_int(event.quantity, "quantity")
                inventory.change(
                    event.product_id,
                    event.zone_id,
                    available_delta=quantity,
                    reserved_delta=-quantity,
                )
            case EventType.INVENTORY_COUNTED:
                inventory.set_available(event.product_id, event.zone_id, event.counted_quantity)
            case EventType.ORDER_CREATED:
                self.handle_order_created(event, inventory, batch, updated_at)
            case EventType.ORDER_COMPLETED:
                self.handle_order_completed(event, inventory, batch, updated_at)

        inventory.flush()
        self.repository.add_processing_records(batch, event, metadata, "PROCESSED", updated_at)
        self.repository.execute_batch(batch)
        return "PROCESSED"

    def record_ignored(
        self,
        event: WarehouseEvent,
        metadata: KafkaMetadata,
        last_sequence: int,
    ) -> None:
        batch = self.repository.new_batch()
        processed_at = datetime.now(UTC)
        self.repository.add_processing_records(
            batch,
            event,
            metadata,
            "IGNORED_OUT_OF_ORDER",
            processed_at,
            error_reason=f"sequence_number={event.sequence_number} <= last_sequence={last_sequence}",
            update_version=False,
        )
        self.repository.execute_batch(batch)

    def handle_order_created(
        self,
        event: WarehouseEvent,
        inventory: "InventoryAccumulator",
        batch,
        updated_at: datetime,
    ) -> None:
        order_id = required_str(event.order_id, "order_id")
        existing_order = self.repository.get_order(order_id)
        if existing_order is not None:
            raise BusinessRuleError(
                "ORDER_ALREADY_EXISTS",
                f"Order already exists: {order_id}",
            )
        for item in event.items:
            inventory.change(
                item.product_id,
                item.zone_id,
                available_delta=-item.quantity,
                reserved_delta=item.quantity,
            )
        self.repository.add_order_update(
            batch,
            event,
            status="CREATED",
            items=event.items,
            created_at=event.event_time,
            completed_at=None,
            updated_at=updated_at,
        )

    def handle_order_completed(
        self,
        event: WarehouseEvent,
        inventory: "InventoryAccumulator",
        batch,
        updated_at: datetime,
    ) -> None:
        order_id = required_str(event.order_id, "order_id")
        order = self.repository.get_order(order_id)
        if order is None:
            raise BusinessRuleError("ORDER_NOT_FOUND", f"Order not found: {order_id}")
        if order.status == "COMPLETED":
            raise BusinessRuleError(
                "ORDER_ALREADY_COMPLETED",
                f"Order already completed: {order_id}",
            )
        for item in order.items:
            inventory.change(item.product_id, item.zone_id, reserved_delta=-item.quantity)
        self.repository.add_order_update(
            batch,
            event,
            status="COMPLETED",
            items=order.items,
            created_at=order.created_at,
            completed_at=event.event_time,
            updated_at=updated_at,
        )


class InventoryAccumulator:
    def __init__(
        self,
        repository: WarehouseRepository,
        event: WarehouseEvent,
        batch,
        updated_at: datetime,
    ) -> None:
        self.repository = repository
        self.event = event
        self.batch = batch
        self.updated_at = updated_at
        self.zone_states: dict[tuple[str, str], InventoryState] = {}
        self.totals: dict[str, InventoryTotal] = {}

    def change(
        self,
        product_id: str | None,
        zone_id: str | None,
        available_delta: int | None = 0,
        reserved_delta: int | None = 0,
    ) -> None:
        if product_id is None or zone_id is None:
            raise EventValidationError("VALIDATION_ERROR", "product_id and zone_id are required")
        state = self.get_zone_state(product_id, zone_id)
        total = self.get_total(product_id)

        new_available = state.available_quantity + (available_delta or 0)
        new_reserved = state.reserved_quantity + (reserved_delta or 0)
        if new_available < 0:
            raise BusinessRuleError(
                "INSUFFICIENT_AVAILABLE",
                f"Available quantity cannot become negative for {product_id} in {zone_id}",
            )
        if new_reserved < 0:
            raise BusinessRuleError(
                "INSUFFICIENT_RESERVED",
                f"Reserved quantity cannot become negative for {product_id} in {zone_id}",
            )

        state.available_quantity = new_available
        state.reserved_quantity = new_reserved
        total.total_available_quantity += available_delta or 0
        total.total_reserved_quantity += reserved_delta or 0

        if total.total_available_quantity < 0:
            raise BusinessRuleError(
                "INSUFFICIENT_TOTAL_AVAILABLE",
                f"Total available quantity cannot become negative for {product_id}",
            )
        if total.total_reserved_quantity < 0:
            raise BusinessRuleError(
                "INSUFFICIENT_TOTAL_RESERVED",
                f"Total reserved quantity cannot become negative for {product_id}",
            )

    def set_available(
        self,
        product_id: str | None,
        zone_id: str | None,
        counted_quantity: int | None,
    ) -> None:
        if counted_quantity is None:
            raise EventValidationError("VALIDATION_ERROR", "counted_quantity is required")
        if product_id is None or zone_id is None:
            raise EventValidationError("VALIDATION_ERROR", "product_id and zone_id are required")

        state = self.get_zone_state(product_id, zone_id)
        delta = counted_quantity - state.available_quantity
        self.change(product_id, zone_id, available_delta=delta)

    def get_zone_state(self, product_id: str, zone_id: str) -> InventoryState:
        key = (product_id, zone_id)
        if key not in self.zone_states:
            self.zone_states[key] = self.repository.get_inventory(product_id, zone_id)
        return self.zone_states[key]

    def get_total(self, product_id: str) -> InventoryTotal:
        if product_id not in self.totals:
            self.totals[product_id] = self.repository.get_inventory_total(product_id)
        return self.totals[product_id]

    def flush(self) -> None:
        for state in self.zone_states.values():
            self.repository.add_inventory_update(self.batch, state, self.event, self.updated_at)
        for total in self.totals.values():
            self.repository.add_total_update(self.batch, total, self.event, self.updated_at)


def required_int(value: int | None, field_name: str) -> int:
    if value is None:
        raise EventValidationError("VALIDATION_ERROR", f"{field_name} is required")
    return value


def required_str(value: str | None, field_name: str) -> str:
    if value is None or value == "":
        raise EventValidationError("VALIDATION_ERROR", f"{field_name} is required")
    return value
