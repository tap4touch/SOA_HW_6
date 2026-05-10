import logging
import time
from typing import Any

from warehouse.settings import Settings

logger = logging.getLogger(__name__)


def create_cluster(settings: Settings) -> Any:
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    load_balancing_policy = TokenAwarePolicy(
        DCAwareRoundRobinPolicy(local_dc=settings.cassandra_local_datacenter)
    )
    return Cluster(
        contact_points=settings.cassandra_hosts,
        port=settings.cassandra_port,
        load_balancing_policy=load_balancing_policy,
    )


def connect_with_retry(settings: Settings) -> tuple[Any, Any]:
    deadline = time.monotonic() + settings.cassandra_connect_timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            cluster = create_cluster(settings)
            session = cluster.connect()
            logger.info("connected to cassandra hosts=%s", ",".join(settings.cassandra_hosts))
            return cluster, session
        except Exception as exc:
            last_error = exc
            logger.info("waiting for cassandra: %s", exc)
            time.sleep(5)

    raise RuntimeError("Cassandra did not become available in time") from last_error
