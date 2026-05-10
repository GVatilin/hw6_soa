import logging
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.common.config import get_settings
from app.common.logging import configure_logging
from app import metrics
from app.cassandra_client import CassandraClient
from app.kafka_worker import KafkaConsumerWorker

configure_logging()
logger = logging.getLogger("consumer-service")
settings = get_settings()

app = FastAPI(title="Consumer Service")

cassandra = CassandraClient(settings)
worker: KafkaConsumerWorker | None = None


@app.on_event("startup")
def startup() -> None:
    global worker
    logger.info("Starting consumer service")
    cassandra.connect_with_retries()
    cassandra.wait_for_quorum()
    worker = KafkaConsumerWorker(settings, cassandra)
    worker.start()


@app.on_event("shutdown")
def shutdown() -> None:
    if worker is not None:
        worker.stop()
    cassandra.close()


@app.get("/metrics")
def prometheus_metrics() -> Response:
    cassandra_ok = cassandra.health_check()
    cassandra_quorum_ok = cassandra.quorum_available()
    metrics.cassandra_available.set(1 if cassandra_ok else 0)
    metrics.cassandra_up_nodes.set(cassandra.up_nodes_count())
    metrics.cassandra_quorum_available.set(1 if cassandra_quorum_ok else 0)
    metrics.consumer_connected.set(1 if worker is not None and worker.connected else 0)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health() -> Dict[str, Any]:
    cassandra_ok = cassandra.health_check()
    cassandra_quorum_ok = cassandra.quorum_available()
    metrics.cassandra_available.set(1 if cassandra_ok else 0)
    metrics.cassandra_up_nodes.set(cassandra.up_nodes_count())
    metrics.cassandra_quorum_available.set(1 if cassandra_quorum_ok else 0)
    kafka_ok = worker is not None and worker.connected
    if not cassandra_ok or not cassandra_quorum_ok or not kafka_ok:
        detail = {
            "status": "unhealthy",
            "kafka_connected": kafka_ok,
            "cassandra_available": cassandra_ok,
            "cassandra_quorum_available": cassandra_quorum_ok,
            "cassandra_up_nodes": cassandra.up_nodes_count(),
            "last_error": None if worker is None else worker.last_error,
        }
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail)
    return {
        "status": "ok",
        "kafka_connected": True,
        "cassandra_available": True,
        "cassandra_quorum_available": True,
        "cassandra_up_nodes": cassandra.up_nodes_count(),
    }


@app.get("/inventory/product/{product_id}/zone/{zone_id}")
def get_inventory_by_product_zone(product_id: str, zone_id: str) -> Dict[str, Any]:
    return cassandra.get_inventory(product_id, zone_id)


@app.get("/inventory/product/{product_id}")
def get_inventory_by_product(product_id: str) -> Dict[str, Any]:
    totals = cassandra.get_product_totals(product_id)
    return {"totals": totals, "zones": cassandra.list_product_zones(product_id)}


@app.get("/inventory/zone/{zone_id}")
def get_inventory_by_zone(zone_id: str) -> Dict[str, Any]:
    return {"zone_id": zone_id, "products": cassandra.list_zone_products(zone_id)}


@app.get("/orders/{order_id}")
def get_order(order_id: str) -> Dict[str, Any]:
    order = cassandra.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
