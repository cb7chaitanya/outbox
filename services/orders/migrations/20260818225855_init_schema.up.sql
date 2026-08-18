-- Orders service schema (spec section 9). This service owns these tables
-- exclusively; no other service reads or writes them.

create type order_status as enum (
  'PENDING',
  'INVENTORY_RESERVED',
  'PAYMENT_AUTHORIZED',
  'READY_FOR_FULFILMENT',
  'COMPLETED',
  'CANCELLING',
  'CANCELLED',
  'MANUAL_REVIEW'
);

create table orders (
  id uuid primary key,
  idempotency_key text not null unique,
  idempotency_request_hash text not null,
  status order_status not null default 'PENDING',
  currency text not null,
  amount_minor bigint not null check (amount_minor >= 0),
  version bigint not null default 1 check (version > 0),
  cancellation_reason text null,
  created_at timestamptz not null,
  updated_at timestamptz not null
);

create table order_items (
  order_id uuid not null references orders (id),
  sku text not null,
  quantity bigint not null check (quantity > 0),
  unit_price_minor bigint not null check (unit_price_minor >= 0),
  primary key (order_id, sku)
);

create table order_transitions (
  id uuid primary key,
  order_id uuid not null references orders (id),
  from_status order_status null,
  to_status order_status not null,
  reason text null,
  triggering_event_id uuid null,
  order_version bigint not null,
  created_at timestamptz not null,
  unique (order_id, order_version)
);

create index order_transitions_order_id_idx on order_transitions (order_id);
