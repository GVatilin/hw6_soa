import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from dateutil.parser import isoparse

from app.common.event_types import WarehouseEventType
from app.cassandra_client import BatchBuilder, CassandraClient, ProcessedStatus

logger = logging.getLogger("event-processor")


class ProcessingError(Exception):
    error_code = "PROCESSING_ERROR"


class ValidationError(ProcessingError):
    error_code = "VALIDATION_ERROR"


class OutOfOrderEvent(Exception):
    pass


def normalize_schema_evolution_fields(event: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(event)
    normalized.setdefault("supplier_id", None)
    normalized.setdefault("metadata", {})
    return normalized


def parse_event_ts(value: str) -> datetime:
    try:
        parsed = isoparse(value)
    except Exception as exc:
        raise ValidationError(f"Invalid event_timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def require(event: Dict[str, Any], field_name: str) -> Any:
    value = event.get(field_name)
    if value in (None, ""):
        raise ValidationError(f"Missing required field: {field_name}")
    return value


def require_positive_quantity(event: Dict[str, Any]) -> int:
    quantity = event.get("quantity")
    if quantity is None:
        raise ValidationError("Missing required field: quantity")
    if not isinstance(quantity, int):
        raise ValidationError(f"Invalid quantity type: {type(quantity).__name__}")
    if quantity <= 0:
        raise ValidationError(f"Invalid quantity: {quantity} (must be positive)")
    return quantity


def require_non_negative_quantity(event: Dict[str, Any]) -> int:
    quantity = event.get("quantity")
    if quantity is None:
        raise ValidationError("Missing required field: quantity")
    if not isinstance(quantity, int):
        raise ValidationError(f"Invalid quantity type: {type(quantity).__name__}")
    if quantity < 0:
        raise ValidationError(f"Invalid counted quantity: {quantity} (must be non-negative)")
    return quantity


def ensure_not_negative(*, available: int, reserved: int, product_id: str, zone_id: str) -> None:
    if available < 0 or reserved < 0:
        raise ValidationError(
            f"Inventory cannot become negative for product={product_id}, zone={zone_id}: "
            f"available={available}, reserved={reserved}"
        )


def affected_entity_ids(event: Dict[str, Any]) -> list[str]:
    entity_ids: list[str] = []
    if event.get("product_id"):
        entity_ids.append(event["product_id"])
    if event.get("order_id"):
        entity_ids.append(event["order_id"])
    if event.get("event_type") == WarehouseEventType.ORDER_CREATED.value:
        for item in event.get("items") or []:
            product_id = item.get("product_id")
            if product_id and product_id not in entity_ids:
                entity_ids.append(product_id)
    return entity_ids or [event["event_id"]]


def check_out_of_order(client: CassandraClient, event: Dict[str, Any], event_ts: datetime) -> None:
    seq = event.get("sequence_number")
    for entity_id in affected_entity_ids(event):
        state = client.get_entity_order_state(entity_id)
        if state is None:
            continue
        last_ts = state.get("last_event_timestamp")
        last_seq = state.get("last_sequence_number")
        if seq is not None and last_seq is not None and seq <= last_seq:
            raise OutOfOrderEvent(f"sequence_number={seq} <= last_sequence_number={last_seq} for entity={entity_id}")
        if seq is None and last_ts is not None and event_ts < last_ts:
            raise OutOfOrderEvent(f"event_timestamp={event_ts.isoformat()} < last_event_timestamp={last_ts.isoformat()} for entity={entity_id}")


def choose_zone_for_reservation(client: CassandraClient, product_id: str, quantity: int, preferred_zone_id: Optional[str]) -> str:
    if preferred_zone_id:
        return preferred_zone_id
    zones = sorted(client.list_product_zones(product_id), key=lambda row: row["zone_id"])
    for zone in zones:
        if int(zone.get("available_quantity") or 0) >= quantity:
            return zone["zone_id"]
    raise ValidationError(f"No zone has enough available inventory for product={product_id}, quantity={quantity}")


def apply_inventory_delta(
    *,
    client: CassandraClient,
    batch: BatchBuilder,
    product_id: str,
    zone_id: str,
    delta_available: int,
    delta_reserved: int,
    supplier_id: Optional[str],
    sequence_number: Optional[int],
) -> None:
    inventory_key = (product_id, zone_id)
    if inventory_key not in batch.inventory_state:
        batch.inventory_state[inventory_key] = client.get_inventory(product_id, zone_id)
    if product_id not in batch.product_state:
        batch.product_state[product_id] = client.get_product_totals(product_id)

    current = batch.inventory_state[inventory_key]
    current_product = batch.product_state[product_id]

    next_available = int(current["available_quantity"] or 0) + delta_available
    next_reserved = int(current["reserved_quantity"] or 0) + delta_reserved
    next_total_available = int(current_product["total_available"] or 0) + delta_available
    next_total_reserved = int(current_product["total_reserved"] or 0) + delta_reserved

    ensure_not_negative(available=next_available, reserved=next_reserved, product_id=product_id, zone_id=zone_id)
    if next_total_available < 0 or next_total_reserved < 0:
        raise ValidationError(
            f"Product totals cannot become negative for product={product_id}: "
            f"available={next_total_available}, reserved={next_total_reserved}"
        )

    effective_supplier_id = supplier_id if supplier_id is not None else current.get("supplier_id") or current_product.get("supplier_id")
    batch.inventory_state[inventory_key] = {
        **current,
        "available_quantity": next_available,
        "reserved_quantity": next_reserved,
        "supplier_id": effective_supplier_id,
    }
    batch.product_state[product_id] = {
        **current_product,
        "total_available": next_total_available,
        "total_reserved": next_total_reserved,
        "supplier_id": effective_supplier_id,
    }
    batch.upsert_inventory(product_id, zone_id, next_available, next_reserved, effective_supplier_id, sequence_number)
    batch.upsert_product_totals(product_id, next_total_available, next_total_reserved, effective_supplier_id, sequence_number)
    batch.insert_event_history(product_id)
    batch.upsert_event_state(product_id, sequence_number)


def reserve_item(
    *,
    client: CassandraClient,
    batch: BatchBuilder,
    product_id: str,
    quantity: int,
    zone_id: Optional[str],
    sequence_number: Optional[int],
) -> str:
    selected_zone_id = choose_zone_for_reservation(client, product_id, quantity, zone_id)
    apply_inventory_delta(
        client=client,
        batch=batch,
        product_id=product_id,
        zone_id=selected_zone_id,
        delta_available=-quantity,
        delta_reserved=quantity,
        supplier_id=None,
        sequence_number=sequence_number,
    )
    return selected_zone_id


def validate_event(event: Dict[str, Any]) -> WarehouseEventType:
    require(event, "event_id")
    require(event, "event_timestamp")
    try:
        event_type = WarehouseEventType(require(event, "event_type"))
    except ValueError as exc:
        raise ValidationError(f"Unsupported event_type: {event.get('event_type')}") from exc
    return event_type


def process_event(client: CassandraClient, event: Dict[str, Any], *, partition: int, offset: int) -> ProcessedStatus:
    event = normalize_schema_evolution_fields(event)
    event_type = validate_event(event)
    event_ts = parse_event_ts(event["event_timestamp"])
    now = utcnow_naive()
    sequence_number = event.get("sequence_number")

    if client.processed_event_exists(event["event_id"]):
        logger.info("duplicate event ignored event_id=%s event_type=%s", event["event_id"], event_type.value)
        return ProcessedStatus.DUPLICATE

    try:
        check_out_of_order(client, event, event_ts)
    except OutOfOrderEvent as exc:
        logger.info("out-of-order event ignored event_id=%s reason=%s", event["event_id"], exc)
        client.mark_processed_only(
            event_id=event["event_id"],
            event_type=event_type.value,
            status=ProcessedStatus.IGNORED_OUT_OF_ORDER,
            processed_at=now,
            partition=partition,
            offset=offset,
            reason=str(exc),
        )
        return ProcessedStatus.IGNORED_OUT_OF_ORDER

    batch = BatchBuilder(client, event, event_ts, now, partition, offset)

    if event_type == WarehouseEventType.PRODUCT_RECEIVED:
        product_id = require(event, "product_id")
        zone_id = require(event, "zone_id")
        quantity = require_positive_quantity(event)
        apply_inventory_delta(
            client=client,
            batch=batch,
            product_id=product_id,
            zone_id=zone_id,
            delta_available=quantity,
            delta_reserved=0,
            supplier_id=event.get("supplier_id"),
            sequence_number=sequence_number,
        )

    elif event_type == WarehouseEventType.PRODUCT_SHIPPED:
        product_id = require(event, "product_id")
        zone_id = require(event, "zone_id")
        quantity = require_positive_quantity(event)
        apply_inventory_delta(
            client=client,
            batch=batch,
            product_id=product_id,
            zone_id=zone_id,
            delta_available=-quantity,
            delta_reserved=0,
            supplier_id=None,
            sequence_number=sequence_number,
        )

    elif event_type == WarehouseEventType.PRODUCT_MOVED:
        product_id = require(event, "product_id")
        from_zone_id = require(event, "from_zone_id")
        to_zone_id = require(event, "to_zone_id")
        quantity = require_positive_quantity(event)
        if from_zone_id == to_zone_id:
            raise ValidationError("from_zone_id and to_zone_id must be different")
        apply_inventory_delta(
            client=client,
            batch=batch,
            product_id=product_id,
            zone_id=from_zone_id,
            delta_available=-quantity,
            delta_reserved=0,
            supplier_id=None,
            sequence_number=sequence_number,
        )
        apply_inventory_delta(
            client=client,
            batch=batch,
            product_id=product_id,
            zone_id=to_zone_id,
            delta_available=quantity,
            delta_reserved=0,
            supplier_id=None,
            sequence_number=sequence_number,
        )

    elif event_type == WarehouseEventType.PRODUCT_RESERVED:
        product_id = require(event, "product_id")
        zone_id = require(event, "zone_id")
        quantity = require_positive_quantity(event)
        reserve_item(
            client=client,
            batch=batch,
            product_id=product_id,
            quantity=quantity,
            zone_id=zone_id,
            sequence_number=sequence_number,
        )

    elif event_type == WarehouseEventType.PRODUCT_RELEASED:
        product_id = require(event, "product_id")
        zone_id = require(event, "zone_id")
        quantity = require_positive_quantity(event)
        apply_inventory_delta(
            client=client,
            batch=batch,
            product_id=product_id,
            zone_id=zone_id,
            delta_available=quantity,
            delta_reserved=-quantity,
            supplier_id=None,
            sequence_number=sequence_number,
        )

    elif event_type == WarehouseEventType.INVENTORY_COUNTED:
        product_id = require(event, "product_id")
        zone_id = require(event, "zone_id")
        counted_quantity = require_non_negative_quantity(event)
        current = client.get_inventory(product_id, zone_id)
        delta_available = counted_quantity - int(current["available_quantity"] or 0)
        apply_inventory_delta(
            client=client,
            batch=batch,
            product_id=product_id,
            zone_id=zone_id,
            delta_available=delta_available,
            delta_reserved=0,
            supplier_id=event.get("supplier_id"),
            sequence_number=sequence_number,
        )

    elif event_type == WarehouseEventType.ORDER_CREATED:
        order_id = require(event, "order_id")
        items = event.get("items") or []
        if not items:
            raise ValidationError("ORDER_CREATED requires at least one item")
        normalized_items = []
        for item in items:
            product_id = item.get("product_id")
            quantity = item.get("quantity")
            if not product_id:
                raise ValidationError("ORDER_CREATED item requires product_id")
            if not isinstance(quantity, int) or quantity <= 0:
                raise ValidationError(f"ORDER_CREATED item has invalid quantity: {quantity}")
            selected_zone_id = reserve_item(
                client=client,
                batch=batch,
                product_id=product_id,
                quantity=quantity,
                zone_id=item.get("zone_id"),
                sequence_number=sequence_number,
            )
            normalized_items.append({"product_id": product_id, "quantity": quantity, "zone_id": selected_zone_id})
        batch.upsert_order(order_id, "CREATED", normalized_items)
        batch.upsert_event_state(order_id, sequence_number)

    elif event_type == WarehouseEventType.ORDER_COMPLETED:
        order_id = require(event, "order_id")
        order = client.get_order(order_id)
        if order is None:
            raise ValidationError(f"ORDER_COMPLETED references unknown order_id={order_id}")
        if order["status"] == "COMPLETED":
            logger.info("order already completed order_id=%s", order_id)
        else:
            import json

            items = json.loads(order["items_json"])
            for item in items:
                apply_inventory_delta(
                    client=client,
                    batch=batch,
                    product_id=item["product_id"],
                    zone_id=item["zone_id"],
                    delta_available=0,
                    delta_reserved=-int(item["quantity"]),
                    supplier_id=None,
                    sequence_number=sequence_number,
                )
            batch.upsert_order(order_id, "COMPLETED", items)
        batch.upsert_event_state(order_id, sequence_number)

    batch.mark_processed(ProcessedStatus.PROCESSED)
    client.execute_logged_batch(batch.operations)
    return ProcessedStatus.PROCESSED
