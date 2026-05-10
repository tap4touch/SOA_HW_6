CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
  product_id text,
  zone_id text,
  available_quantity int,
  reserved_quantity int,
  last_event_id text,
  last_sequence_number bigint,
  updated_at timestamp,
  PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
  product_id text,
  total_available_quantity int,
  total_reserved_quantity int,
  last_event_id text,
  last_sequence_number bigint,
  updated_at timestamp,
  PRIMARY KEY ((product_id))
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
  zone_id text,
  product_id text,
  available_quantity int,
  reserved_quantity int,
  last_event_id text,
  last_sequence_number bigint,
  updated_at timestamp,
  PRIMARY KEY ((zone_id), product_id)
);
