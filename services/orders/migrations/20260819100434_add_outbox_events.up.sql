-- Transactional outbox table (spec section 9, shared infrastructure shape).
-- Inserted in the same transaction as the business mutation that requires
-- an event (invariant I3); a separate publisher worker claims and publishes
-- rows independently of the request path (spec section 13).

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

-- Publisher claim query filters on unpublished rows ordered by age; this
-- index makes that scan cheap once the table has real volume.
create index outbox_events_unpublished_idx on outbox_events (created_at, id)
  where published_at is null;
