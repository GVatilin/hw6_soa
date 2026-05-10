import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from app.common.avro import available_schema_versions, latest_schema_version, make_avro_serializer, schema_as_json
from app.common.config import get_settings
from app.common.event_types import WarehouseEventType
from app.common.logging import configure_logging

configure_logging()
logger = logging.getLogger("wms-service")
settings = get_settings()

app = FastAPI(title="WMS Service")

producer: Producer | None = None
key_serializer = StringSerializer("utf_8")
avro_serializers: Dict[int, Any] = {}


class OrderItemIn(BaseModel):
    product_id: str
    quantity: int
    zone_id: Optional[str] = None


class WarehouseEventIn(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: WarehouseEventType
    product_id: Optional[str] = None
    quantity: Optional[int] = None
    zone_id: Optional[str] = None
    supplier_id: Optional[str] = None
    from_zone_id: Optional[str] = None
    to_zone_id: Optional[str] = None
    order_id: Optional[str] = None
    items: List[OrderItemIn] = Field(default_factory=list)
    event_timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    sequence_number: Optional[int] = None
    metadata: Dict[str, str] = Field(default_factory=dict)


class ProduceResponse(BaseModel):
    status: str
    topic: str
    partition: int
    offset: int
    event_id: str
    event_type: str
    schema_version: int


def _event_to_avro_dict(event: WarehouseEventIn, schema_version: int) -> Dict[str, Any]:
    payload = event.model_dump(mode="json")
    payload["items"] = [item.model_dump(mode="json") for item in event.items]
    payload.setdefault("metadata", {})
    if schema_version <= 1:
        payload.pop("supplier_id", None)
    else:
        payload.setdefault("supplier_id", None)
    for field_name in [
        "product_id",
        "quantity",
        "zone_id",
        "from_zone_id",
        "to_zone_id",
        "order_id",
        "sequence_number",
    ]:
        payload.setdefault(field_name, None)
    return payload


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
    else:
        logger.info(
            "event delivered topic=%s partition=%s offset=%s key=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key(),
        )


@app.on_event("startup")
def startup() -> None:
    global producer, avro_serializers
    logger.info("Starting WMS service")
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    avro_serializers = {
        version: make_avro_serializer(settings.schema_registry_url, version)
        for version in available_schema_versions()
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "wms-service", "schema_versions": available_schema_versions()}


@app.post("/events", response_model=ProduceResponse)
def publish_event(
    event: WarehouseEventIn,
    schema_version: int | None = Query(default=None, ge=1, description="Schema version to use"),
) -> ProduceResponse:
    if producer is None:
        raise HTTPException(status_code=503, detail="Producer is not ready")
    schema_version = schema_version or latest_schema_version()
    if schema_version not in avro_serializers:
        raise HTTPException(status_code=400, detail=f"Schema version {schema_version} is not registered in this service")

    key = event.product_id or event.order_id or event.event_id
    value = _event_to_avro_dict(event, schema_version)
    ctx_key = SerializationContext(settings.kafka_topic, MessageField.KEY)
    ctx_value = SerializationContext(settings.kafka_topic, MessageField.VALUE)

    try:
        producer.produce(
            topic=settings.kafka_topic,
            key=key_serializer(key, ctx_key),
            value=avro_serializers[schema_version](value, ctx_value),
            on_delivery=_delivery_report,
        )
        producer.flush(10)
    except Exception as exc:
        logger.exception("Could not produce event")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ProduceResponse(
        status="published",
        topic=settings.kafka_topic,
        partition=-1,
        offset=-1,
        event_id=event.event_id,
        event_type=event.event_type.value,
        schema_version=schema_version,
    )
