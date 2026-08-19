-- Transactional outbox table (spec section 9, shared infrastructure
-- shape) — identical shape to orders'/inventory's copies; each service
-- owns its own.

create table outbox_events (
  id uuid primary key,
  aggregate_type text not null,
  aggregate_id uuid not null,
  aggregate_version bigint not null,
  topic text not null,
  message_key text not null,
  envelope jsonb not null,
  created_at timestamptz not null,
  published_at timestamptz null,
  attempts int not null default 0,
  next_attempt_at timestamptz not null,
  last_error text null,
  claimed_by text null,
  claimed_until timestamptz null,
  unique (aggregate_type, aggregate_id, aggregate_version, topic)
);

create index outbox_events_unpublished_idx on outbox_events (created_at, id)
  where published_at is null;
