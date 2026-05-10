import logging
from pathlib import Path
from typing import Any

from cassandra import ConsistencyLevel  # type: ignore[attr-defined]
from cassandra.query import SimpleStatement

from warehouse.settings import Settings

logger = logging.getLogger(__name__)


def apply_migrations(session: Any, settings: Settings) -> None:
    files = sorted(Path(settings.migrations_path).glob("*.cql"))
    for file_path in files:
        cql = file_path.read_text(encoding="utf-8")
        statements = [statement.strip() for statement in cql.split(";") if statement.strip()]
        for statement in statements:
            session.execute(SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM))
        logger.info("applied cassandra migration=%s", file_path)
    session.set_keyspace(settings.cassandra_keyspace)
