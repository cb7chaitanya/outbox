-- Idempotent-inbox tables (spec section 9, shared infrastructure shape),
-- identical shape to inventory's/payments' copies. Orders starts consuming
-- in M05: reservation outcomes from `inventory.events.v1`, to react and
-- trigger `payments.authorize_payment` (see
-- docs/adr/0010-orders-consumes-reservation-outcomes.md).

create table inbox_events (
  consumer_name text not null,
  event_id uuid not null,
  source_topic text not null,
  source_partition int not null,
  source_offset bigint not null,
  aggregate_id uuid not null,
  aggregate_version bigint not null,
  received_at timestamptz not null,
  processed_at timestamptz null,
  payload_hash text not null,
  primary key (consumer_name, event_id)
);

create table consumer_aggregate_versions (
  consumer_name text not null,
  aggregate_id uuid not null,
  last_version bigint not null,
  primary key (consumer_name, aggregate_id)
);

create table consumer_offsets (
  consumer_name text not null,
  topic text not null,
  partition int not null,
  next_offset bigint not null,
  updated_at timestamptz not null,
  primary key (consumer_name, topic, partition)
);
