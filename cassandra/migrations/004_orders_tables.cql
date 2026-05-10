CREATE TABLE IF NOT EXISTS warehouse.orders_by_id (
  order_id text,
  status text,
  items_json text,
  created_at timestamp,
  completed_at timestamp,
  last_event_id text,
  last_sequence_number bigint,
  PRIMARY KEY ((order_id))
);
