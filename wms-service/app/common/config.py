from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = Field("localhost:29092", alias="KAFKA_BOOTSTRAP_SERVERS")
    schema_registry_url: str = Field("http://localhost:8081", alias="SCHEMA_REGISTRY_URL")
    kafka_topic: str = Field("warehouse-events", alias="KAFKA_TOPIC")
    kafka_dlq_topic: str = Field("warehouse-events-dlq", alias="KAFKA_DLQ_TOPIC")
    kafka_consumer_group: str = Field("warehouse-state-consumer", alias="KAFKA_CONSUMER_GROUP")

    cassandra_contact_points_raw: str = Field("localhost", alias="CASSANDRA_CONTACT_POINTS")
    cassandra_keyspace: str = Field("warehouse", alias="CASSANDRA_KEYSPACE")
    cassandra_local_dc: str = Field("datacenter1", alias="CASSANDRA_LOCAL_DC")
    cassandra_write_cl: str = Field("QUORUM", alias="CASSANDRA_WRITE_CL")
    cassandra_read_cl: str = Field("ONE", alias="CASSANDRA_READ_CL")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    @property
    def cassandra_contact_points(self) -> List[str]:
        return [p.strip() for p in self.cassandra_contact_points_raw.split(",") if p.strip()]


def get_settings() -> Settings:
    return Settings()
