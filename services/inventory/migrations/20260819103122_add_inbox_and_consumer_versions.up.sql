-- Idempotent-inbox tables (spec section 9, shared infrastructure shape):
-- `(consumer_name, event_id)` backs invariant I11 (single-application per
-- consumer); `consumer_aggregate_versions` backs the stale/gap/apply
-- ordering policy (spec section 14) per (consumer, source aggregate).

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
