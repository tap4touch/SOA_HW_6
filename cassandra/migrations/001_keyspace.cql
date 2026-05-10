CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
}
AND durable_writes = true;
