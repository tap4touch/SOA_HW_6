from datetime import UTC, datetime
from enum import StrEnum
from time import time_ns
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    PRODUCT_RECEIVED = "PRODUCT_RECEIVED"
    PRODUCT_SHIPPED = "PRODUCT_SHIPPED"
    PRODUCT_MOVED = "PRODUCT_MOVED"
    PRODUCT_RESERVED = "PRODUCT_RESERVED"
    PRODUCT_RELEASED = "PRODUCT_RELEASED"
    INVENTORY_COUNTED = "INVENTORY_COUNTED"
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_COMPLETED = "ORDER_COMPLETED"


class OrderItem(BaseModel):
    product_id: str
    zone_id: str
    quantity: int


class WarehouseEvent(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    event_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sequence_number: int = Field(default_factory=lambda: time_ns())
    entity_key: str | None = None

    product_id: str | None = None
    zone_id: str | None = None
    from_zone_id: str | None = None
    to_zone_id: str | None = None
    quantity: int | None = None
    counted_quantity: int | None = None

    order_id: str | None = None
    items: list[OrderItem] = Field(default_factory=list)

    @classmethod
    def from_avro_dict(cls, payload: dict) -> "WarehouseEvent":
        return cls.model_validate(payload)

    def to_avro_dict(self) -> dict:
        data = self.model_dump(mode="json")
        data["event_type"] = str(self.event_type)
        return data

    def topic_key(self) -> str:
        if self.product_id:
            return self.product_id
        if self.items:
            return self.items[0].product_id
        if self.order_id:
            return self.order_id
        return self.event_id

    def resolved_entity_key(self) -> str:
        if self.entity_key:
            return self.entity_key
        if self.product_id:
            return f"product:{self.product_id}"
        if self.order_id:
            return f"order:{self.order_id}"
        if self.items:
            item_keys = ",".join(f"{item.product_id}:{item.zone_id}" for item in self.items)
            return f"order-items:{item_keys}"
        return f"event:{self.event_id}"

    def history_product_id(self) -> str:
        if self.product_id:
            return self.product_id
        if self.items:
            return self.items[0].product_id
        if self.order_id:
            return f"ORDER:{self.order_id}"
        return "UNKNOWN"
