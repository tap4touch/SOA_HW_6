from warehouse.events import EventType, WarehouseEvent


class EventValidationError(Exception):
    def __init__(self, error_code: str, reason: str) -> None:
        super().__init__(reason)
        self.error_code = error_code
        self.reason = reason


def validate_event(event: WarehouseEvent) -> None:
    match EventType(event.event_type):
        case EventType.PRODUCT_RECEIVED:
            require_product_zone_quantity(event)
        case EventType.PRODUCT_SHIPPED:
            require_product_zone_quantity(event)
        case EventType.PRODUCT_RESERVED:
            require_product_zone_quantity(event)
        case EventType.PRODUCT_RELEASED:
            require_product_zone_quantity(event)
        case EventType.PRODUCT_MOVED:
            require_product(event)
            require_positive_quantity(event.quantity)
            require_field(event.from_zone_id, "from_zone_id")
            require_field(event.to_zone_id, "to_zone_id")
            if event.from_zone_id == event.to_zone_id:
                raise EventValidationError(
                    "VALIDATION_ERROR",
                    "from_zone_id and to_zone_id must be different for PRODUCT_MOVED",
                )
        case EventType.INVENTORY_COUNTED:
            require_field(event.product_id, "product_id")
            require_field(event.zone_id, "zone_id")
            require_non_negative_count(event.counted_quantity)
        case EventType.ORDER_CREATED:
            require_field(event.order_id, "order_id")
            if not event.items:
                raise EventValidationError("VALIDATION_ERROR", "ORDER_CREATED requires items")
            for item in event.items:
                require_field(item.product_id, "item.product_id")
                require_field(item.zone_id, "item.zone_id")
                require_positive_quantity(item.quantity)
        case EventType.ORDER_COMPLETED:
            require_field(event.order_id, "order_id")


def require_product_zone_quantity(event: WarehouseEvent) -> None:
    require_product(event)
    require_field(event.zone_id, "zone_id")
    require_positive_quantity(event.quantity)


def require_product(event: WarehouseEvent) -> None:
    require_field(event.product_id, "product_id")


def require_field(value: str | None, field_name: str) -> None:
    if value is None or value == "":
        raise EventValidationError("VALIDATION_ERROR", f"{field_name} is required")


def require_positive_quantity(value: int | None) -> None:
    if value is None:
        raise EventValidationError("VALIDATION_ERROR", "quantity is required")
    if value <= 0:
        raise EventValidationError(
            "VALIDATION_ERROR",
            f"Invalid quantity: {value} (must be positive)",
        )


def require_non_negative_count(value: int | None) -> None:
    if value is None:
        raise EventValidationError("VALIDATION_ERROR", "counted_quantity is required")
    if value < 0:
        raise EventValidationError(
            "VALIDATION_ERROR",
            f"Invalid counted_quantity: {value} (must be non-negative)",
        )
