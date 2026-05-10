from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_host: str = "127.0.0.1"
    api_port: int = 8000

    kafka_bootstrap_servers: str = "localhost:29092"
    schema_registry_url: str = "http://localhost:8081"
    warehouse_events_topic: str = "warehouse-events"
    warehouse_dlq_topic: str = "warehouse-events-dlq"
    warehouse_consumer_group: str = "warehouse-state-consumer"
    kafka_partitions: int = 3
    kafka_replication_factor: int = 1

    cassandra_contact_points: str = "127.0.0.1"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "warehouse"
    cassandra_local_datacenter: str = "dc1"
    cassandra_read_consistency: str = "QUORUM"
    cassandra_write_consistency: str = "QUORUM"
    cassandra_connect_timeout_seconds: int = 180

    schema_path: Path = Field(default=Path("schemas/warehouse_event.avsc"))
    migrations_path: Path = Field(default=Path("cassandra/migrations"))

    @property
    def cassandra_hosts(self) -> list[str]:
        return [host.strip() for host in self.cassandra_contact_points.split(",") if host.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
