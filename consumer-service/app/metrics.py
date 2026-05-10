from prometheus_client import Counter, Gauge, Histogram

consumer_lag = Gauge(
    "consumer_lag",
    "Kafka consumer lag by topic partition",
    ["topic", "partition"],
)

events_processed_total = Counter(
    "events_processed_total",
    "Number of warehouse events successfully processed",
    ["event_type", "status"],
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing one Kafka event",
    ["event_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Number of Cassandra write errors",
)

dlq_events_total = Counter(
    "dlq_events_total",
    "Number of events sent to the dead letter queue",
    ["error_code"],
)

consumer_connected = Gauge(
    "consumer_connected",
    "1 when the consumer loop is polling Kafka, otherwise 0",
)

cassandra_available = Gauge(
    "cassandra_available",
    "1 when Cassandra health check succeeds, otherwise 0",
)

cassandra_up_nodes = Gauge(
    "cassandra_up_nodes",
    "Number of Cassandra nodes currently marked up by the driver",
)

cassandra_quorum_available = Gauge(
    "cassandra_quorum_available",
    "1 when enough Cassandra nodes are up to satisfy QUORUM writes, otherwise 0",
)
