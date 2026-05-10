import base64
import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter

from app.common.avro import make_avro_deserializer, wait_for_schema_registry
from app.common.config import Settings
from app import metrics
from app.cassandra_client import CassandraClient, ProcessedStatus
from app.processor import ProcessingError, process_event

logger = logging.getLogger("kafka-worker")


class KafkaConsumerWorker:
    def __init__(self, settings: Settings, cassandra: CassandraClient) -> None:
        self.settings = settings
        self.cassandra = cassandra
        self.consumer: Consumer | None = None
        self.dlq_producer: Producer | None = None
        self.deserializer = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self.connected = False
        self.last_error: str | None = None

    def start(self) -> None:
        wait_for_schema_registry(self.settings.schema_registry_url)
        self.deserializer = make_avro_deserializer(self.settings.schema_registry_url)
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": self.settings.kafka_consumer_group,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": 300000,
                "session.timeout.ms": 45000,
            }
        )
        self.dlq_producer = Producer({"bootstrap.servers": self.settings.kafka_bootstrap_servers})
        self.consumer.subscribe([self.settings.kafka_topic])
        self._thread = threading.Thread(target=self._run_loop, name="warehouse-kafka-consumer", daemon=True)
        self._thread.start()
        logger.info(
            "Kafka consumer started topic=%s group=%s",
            self.settings.kafka_topic,
            self.settings.kafka_consumer_group,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=15)
        if self.consumer:
            self.consumer.close()
        if self.dlq_producer:
            self.dlq_producer.flush(5)
        self.connected = False
        metrics.consumer_connected.set(0)

    def _run_loop(self) -> None:
        assert self.consumer is not None
        assert self.deserializer is not None
        self.connected = True
        metrics.consumer_connected.set(1)
        while not self._stop_event.is_set():
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(msg.error()))

                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                try:
                    event = self.deserializer(msg.value(), ctx)
                except Exception as exc:
                    self._send_dlq(
                        original_event={"raw_value_base64": base64.b64encode(msg.value() or b"").decode("ascii")},
                        error_reason=f"Avro deserialization failed: {exc}",
                        error_code="DESERIALIZATION_ERROR",
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    self.consumer.commit(message=msg, asynchronous=False)
                    self._update_lag(msg.topic(), msg.partition(), msg.offset())
                    continue

                start = time.perf_counter()
                event_type = str(event.get("event_type", "UNKNOWN"))
                try:
                    status = process_event(self.cassandra, event, partition=msg.partition(), offset=msg.offset())
                    duration = time.perf_counter() - start
                    metrics.event_processing_duration_seconds.labels(event_type=event_type).observe(duration)
                    metrics.events_processed_total.labels(event_type=event_type, status=status.value).inc()
                    logger.info(
                        "processed event_id=%s event_type=%s status=%s partition=%s offset=%s",
                        event.get("event_id"),
                        event_type,
                        status.value,
                        msg.partition(),
                        msg.offset(),
                    )
                    self.consumer.commit(message=msg, asynchronous=False)
                except ProcessingError as exc:
                    self._send_dlq(
                        original_event=event,
                        error_reason=str(exc),
                        error_code=getattr(exc, "error_code", "PROCESSING_ERROR"),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    self.consumer.commit(message=msg, asynchronous=False)
                    metrics.events_processed_total.labels(event_type=event_type, status=ProcessedStatus.DLQ.value).inc()
                except Exception as exc:
                    metrics.cassandra_write_errors_total.inc()
                    logger.exception("Unexpected processing error, sending event to DLQ")
                    self._send_dlq(
                        original_event=event,
                        error_reason=str(exc),
                        error_code="UNEXPECTED_PROCESSING_ERROR",
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    self.consumer.commit(message=msg, asynchronous=False)
                    metrics.events_processed_total.labels(event_type=event_type, status=ProcessedStatus.DLQ.value).inc()
                finally:
                    self._update_lag(msg.topic(), msg.partition(), msg.offset())

            except Exception as exc:
                self.connected = False
                metrics.consumer_connected.set(0)
                self.last_error = str(exc)
                logger.exception("Kafka consumer loop error")
                time.sleep(5)
                self.connected = True
                metrics.consumer_connected.set(1)

    def _send_dlq(
        self,
        *,
        original_event: Dict[str, Any],
        error_reason: str,
        error_code: str,
        partition: int,
        offset: int,
    ) -> None:
        assert self.dlq_producer is not None
        dlq_payload = {
            "original_event": original_event,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "kafka_metadata": {"partition": partition, "offset": offset},
        }
        self.dlq_producer.produce(
            self.settings.kafka_dlq_topic,
            key=str(original_event.get("event_id") or f"{partition}-{offset}").encode("utf-8"),
            value=json.dumps(dlq_payload, ensure_ascii=False, default=str).encode("utf-8"),
        )
        self.dlq_producer.flush(10)
        metrics.dlq_events_total.labels(error_code=error_code).inc()
        logger.warning("event sent to DLQ error_code=%s reason=%s partition=%s offset=%s", error_code, error_reason, partition, offset)

    def _update_lag(self, topic: str, partition: int, processed_offset: int) -> None:
        if self.consumer is None:
            return
        try:
            _low, high = self.consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=2, cached=False)
            lag = max(high - processed_offset - 1, 0)
            metrics.consumer_lag.labels(topic=topic, partition=str(partition)).set(lag)
        except Exception as exc:
            logger.debug("Could not update consumer lag: %s", exc)
