import json
import logging
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Tuple

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, PreparedStatement, SimpleStatement

from app.common.config import Settings

logger = logging.getLogger("cassandra-client")


CONSISTENCY_LEVELS = {
    "ANY": ConsistencyLevel.ANY,
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
    "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
}


class ProcessedStatus(str, Enum):
    PROCESSED = "PROCESSED"
    DUPLICATE = "DUPLICATE"
    IGNORED_OUT_OF_ORDER = "IGNORED_OUT_OF_ORDER"
    DLQ = "DLQ"


def parse_cl(value: str) -> int:
    try:
        return CONSISTENCY_LEVELS[value.upper()]
    except KeyError as exc:
        raise ValueError(f"Unknown Cassandra consistency level: {value}") from exc


class CassandraClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.write_cl = parse_cl(settings.cassandra_write_cl)
        self.read_cl = parse_cl(settings.cassandra_read_cl)
        self.cluster: Cluster | None = None
        self.session: Session | None = None
        self._prepared: dict[str, PreparedStatement] = {}

    def connect_with_retries(self, attempts: int = 60, delay_seconds: int = 5) -> None:
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self.cluster = Cluster(
                    contact_points=self.settings.cassandra_contact_points,
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.settings.cassandra_local_dc),
                )
                self.session = self.cluster.connect()
                self.apply_migrations()
                self.session.set_keyspace(self.settings.cassandra_keyspace)
                self.prepare_statements()
                logger.info("Connected to Cassandra contact_points=%s", self.settings.cassandra_contact_points)
                return
            except Exception as exc:
                last_error = exc
                logger.warning("Cassandra is not ready yet, attempt %s/%s: %s", attempt, attempts, exc)
                time.sleep(delay_seconds)
        raise RuntimeError(f"Could not connect to Cassandra: {last_error}")

    def close(self) -> None:
        if self.cluster:
            self.cluster.shutdown()

    def apply_migrations(self) -> None:
        assert self.session is not None
        keyspace = self.settings.cassandra_keyspace
        dc = self.settings.cassandra_local_dc
        statements = [
            f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': 3}}
            AND durable_writes = true
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.inventory_by_product_zone (
                product_id text,
                zone_id text,
                available_quantity int,
                reserved_quantity int,
                supplier_id text,
                last_event_timestamp timestamp,
                last_sequence_number bigint,
                updated_at timestamp,
                PRIMARY KEY ((product_id), zone_id)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.inventory_by_product (
                product_id text PRIMARY KEY,
                total_available int,
                total_reserved int,
                supplier_id text,
                last_event_timestamp timestamp,
                last_sequence_number bigint,
                updated_at timestamp
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.inventory_by_zone (
                zone_id text,
                product_id text,
                available_quantity int,
                reserved_quantity int,
                supplier_id text,
                last_event_timestamp timestamp,
                last_sequence_number bigint,
                updated_at timestamp,
                PRIMARY KEY ((zone_id), product_id)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.processed_events (
                event_id text PRIMARY KEY,
                event_type text,
                status text,
                processed_at timestamp,
                source_partition int,
                source_offset bigint,
                reason text
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.event_order_state (
                entity_id text PRIMARY KEY,
                last_event_timestamp timestamp,
                last_sequence_number bigint,
                last_event_id text
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.orders_by_id (
                order_id text PRIMARY KEY,
                status text,
                items_json text,
                created_at timestamp,
                updated_at timestamp,
                last_event_timestamp timestamp
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.event_history_by_product (
                product_id text,
                event_timestamp timestamp,
                event_id text,
                event_type text,
                payload_json text,
                PRIMARY KEY ((product_id), event_timestamp, event_id)
            ) WITH CLUSTERING ORDER BY (event_timestamp DESC)
            """,
        ]
        for cql in statements:
            statement = SimpleStatement(cql.strip(), consistency_level=ConsistencyLevel.ONE)
            self.session.execute(statement)
        logger.info("Cassandra migrations applied for keyspace=%s", keyspace)

    def prepare_statements(self) -> None:
        assert self.session is not None
        queries = {
            "select_inventory_pz": "SELECT product_id, zone_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_sequence_number FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?",
            "select_product": "SELECT product_id, total_available, total_reserved, supplier_id, last_event_timestamp, last_sequence_number FROM inventory_by_product WHERE product_id = ?",
            "select_zones_for_product": "SELECT product_id, zone_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_sequence_number FROM inventory_by_product_zone WHERE product_id = ?",
            "select_zone_products": "SELECT zone_id, product_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_sequence_number FROM inventory_by_zone WHERE zone_id = ?",
            "select_processed": "SELECT event_id, status FROM processed_events WHERE event_id = ?",
            "select_order_state": "SELECT entity_id, last_event_timestamp, last_sequence_number FROM event_order_state WHERE entity_id = ?",
            "select_order": "SELECT order_id, status, items_json, created_at, updated_at, last_event_timestamp FROM orders_by_id WHERE order_id = ?",
            "upsert_inventory_pz": "INSERT INTO inventory_by_product_zone (product_id, zone_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_sequence_number, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            "upsert_product": "INSERT INTO inventory_by_product (product_id, total_available, total_reserved, supplier_id, last_event_timestamp, last_sequence_number, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            "upsert_zone": "INSERT INTO inventory_by_zone (zone_id, product_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp, last_sequence_number, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            "upsert_processed": "INSERT INTO processed_events (event_id, event_type, status, processed_at, source_partition, source_offset, reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
            "upsert_order_state": "INSERT INTO event_order_state (entity_id, last_event_timestamp, last_sequence_number, last_event_id) VALUES (?, ?, ?, ?)",
            "upsert_order": "INSERT INTO orders_by_id (order_id, status, items_json, created_at, updated_at, last_event_timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            "insert_event_history": "INSERT INTO event_history_by_product (product_id, event_timestamp, event_id, event_type, payload_json) VALUES (?, ?, ?, ?, ?)",
        }
        for name, query in queries.items():
            prepared = self.session.prepare(query)
            if name.startswith("select"):
                prepared.consistency_level = self.read_cl
            else:
                prepared.consistency_level = self.write_cl
            self._prepared[name] = prepared

    def execute(self, name: str, params: Tuple[Any, ...]) -> Any:
        assert self.session is not None
        return self.session.execute(self._prepared[name], params)

    def get_inventory(self, product_id: str, zone_id: str) -> Dict[str, Any]:
        row = self.execute("select_inventory_pz", (product_id, zone_id)).one()
        if row is None:
            return {
                "product_id": product_id,
                "zone_id": zone_id,
                "available_quantity": 0,
                "reserved_quantity": 0,
                "supplier_id": None,
                "last_event_timestamp": None,
                "last_sequence_number": None,
            }
        return dict(row._asdict())

    def get_product_totals(self, product_id: str) -> Dict[str, Any]:
        row = self.execute("select_product", (product_id,)).one()
        if row is None:
            return {
                "product_id": product_id,
                "total_available": 0,
                "total_reserved": 0,
                "supplier_id": None,
                "last_event_timestamp": None,
                "last_sequence_number": None,
            }
        return dict(row._asdict())

    def list_product_zones(self, product_id: str) -> list[Dict[str, Any]]:
        rows = self.execute("select_zones_for_product", (product_id,))
        return [dict(row._asdict()) for row in rows]

    def list_zone_products(self, zone_id: str) -> list[Dict[str, Any]]:
        rows = self.execute("select_zone_products", (zone_id,))
        return [dict(row._asdict()) for row in rows]

    def processed_event_exists(self, event_id: str) -> bool:
        return self.execute("select_processed", (event_id,)).one() is not None

    def get_entity_order_state(self, entity_id: str) -> Optional[Dict[str, Any]]:
        row = self.execute("select_order_state", (entity_id,)).one()
        return None if row is None else dict(row._asdict())

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        row = self.execute("select_order", (order_id,)).one()
        return None if row is None else dict(row._asdict())

    def execute_logged_batch(self, operations: Iterable[Tuple[str, Tuple[Any, ...]]]) -> None:
        assert self.session is not None
        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.write_cl)
        for name, params in operations:
            batch.add(self._prepared[name], params)
        self.session.execute(batch)

    def mark_processed_only(
        self,
        *,
        event_id: str,
        event_type: str,
        status: ProcessedStatus,
        processed_at: datetime,
        partition: int,
        offset: int,
        reason: str | None = None,
    ) -> None:
        self.execute_logged_batch(
            [
                (
                    "upsert_processed",
                    (event_id, event_type, status.value, processed_at, partition, offset, reason),
                )
            ]
        )

    def health_check(self) -> bool:
        if self.session is None:
            return False
        try:
            self.session.execute(SimpleStatement("SELECT now() FROM system.local", consistency_level=ConsistencyLevel.ONE))
            return True
        except Exception:
            logger.exception("Cassandra health check failed")
            return False

    def up_nodes_count(self) -> int:
        if self.cluster is None:
            return 0
        metadata = getattr(self.cluster, "metadata", None)
        if metadata is None:
            return 0
        return sum(
            1
            for host in metadata.all_hosts()
            if host.is_up and getattr(host, "datacenter", None) == self.settings.cassandra_local_dc
        )

    def quorum_required_nodes(self) -> int:
        return 2

    def quorum_available(self) -> bool:
        return self.up_nodes_count() >= self.quorum_required_nodes()

    def wait_for_quorum(self, attempts: int = 60, delay_seconds: int = 5) -> None:
        for attempt in range(1, attempts + 1):
            up_nodes = self.up_nodes_count()
            if up_nodes >= self.quorum_required_nodes():
                logger.info("Cassandra QUORUM is available up_nodes=%s", up_nodes)
                return
            logger.warning(
                "Waiting for Cassandra QUORUM, attempt %s/%s: up_nodes=%s required=%s",
                attempt,
                attempts,
                up_nodes,
                self.quorum_required_nodes(),
            )
            time.sleep(delay_seconds)
        raise RuntimeError(
            f"Cassandra QUORUM is not available: up_nodes={self.up_nodes_count()} required={self.quorum_required_nodes()}"
        )


class BatchBuilder:
    def __init__(self, client: CassandraClient, event: Dict[str, Any], event_ts: datetime, now: datetime, partition: int, offset: int) -> None:
        self.client = client
        self.event = event
        self.event_ts = event_ts
        self.now = now
        self.partition = partition
        self.offset = offset
        self.operations: list[Tuple[str, Tuple[Any, ...]]] = []
        self.inventory_state: dict[tuple[str, str], Dict[str, Any]] = {}
        self.product_state: dict[str, Dict[str, Any]] = {}

    def upsert_inventory(self, product_id: str, zone_id: str, available: int, reserved: int, supplier_id: str | None, seq: int | None) -> None:
        self.operations.append(
            (
                "upsert_inventory_pz",
                (product_id, zone_id, available, reserved, supplier_id, self.event_ts, seq, self.now),
            )
        )
        self.operations.append(
            (
                "upsert_zone",
                (zone_id, product_id, available, reserved, supplier_id, self.event_ts, seq, self.now),
            )
        )

    def upsert_product_totals(self, product_id: str, total_available: int, total_reserved: int, supplier_id: str | None, seq: int | None) -> None:
        self.operations.append(
            (
                "upsert_product",
                (product_id, total_available, total_reserved, supplier_id, self.event_ts, seq, self.now),
            )
        )

    def upsert_event_state(self, entity_id: str, seq: int | None) -> None:
        self.operations.append(("upsert_order_state", (entity_id, self.event_ts, seq, self.event["event_id"])))

    def upsert_order(self, order_id: str, status: str, items: list[dict[str, Any]]) -> None:
        self.operations.append(
            (
                "upsert_order",
                (order_id, status, json.dumps(items, ensure_ascii=False), self.event_ts, self.now, self.event_ts),
            )
        )

    def insert_event_history(self, product_id: str) -> None:
        self.operations.append(
            (
                "insert_event_history",
                (
                    product_id,
                    self.event_ts,
                    self.event["event_id"],
                    self.event["event_type"],
                    json.dumps(self.event, ensure_ascii=False, default=str),
                ),
            )
        )

    def mark_processed(self, status: ProcessedStatus = ProcessedStatus.PROCESSED, reason: str | None = None) -> None:
        self.operations.append(
            (
                "upsert_processed",
                (
                    self.event["event_id"],
                    self.event["event_type"],
                    status.value,
                    self.now,
                    self.partition,
                    self.offset,
                    reason,
                ),
            )
        )
