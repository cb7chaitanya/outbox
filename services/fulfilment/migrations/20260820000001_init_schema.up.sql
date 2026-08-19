-- Fulfilment service schema (spec section 9). This service owns this
-- table exclusively; no other service reads or writes it.

create type fulfilment_status as enum (
  'PENDING',
  'CREATED',
  'FAILED',
  'CANCELLED'
);

create table fulfilments (
  id uuid primary key,
  order_id uuid not null unique,
  reservation_id uuid not null,
  payment_id uuid not null,
  status fulfilment_status not null,
  failure_code text null,
  version bigint not null default 1 check (version > 0),
  created_at timestamptz not null,
  updated_at timestamptz not null
);
