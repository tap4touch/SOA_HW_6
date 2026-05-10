CREATE TABLE IF NOT EXISTS warehouse.processed_events (
  event_id text,
  event_type text,
  entity_key text,
  sequence_number bigint,
  processing_status text,
  processed_at timestamp,
  kafka_partition int,
  kafka_offset bigint,
  PRIMARY KEY ((event_id))
);

CREATE TABLE IF NOT EXISTS warehouse.entity_versions (
  entity_key text,
  last_sequence_number bigint,
  last_event_id text,
  updated_at timestamp,
  PRIMARY KEY ((entity_key))
);

CREATE TABLE IF NOT EXISTS warehouse.event_history_by_product (
  product_id text,
  event_time timestamp,
  event_id text,
  event_type text,
  entity_key text,
  sequence_number bigint,
  processing_status text,
  payload text,
  error_reason text,
  PRIMARY KEY ((product_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);
