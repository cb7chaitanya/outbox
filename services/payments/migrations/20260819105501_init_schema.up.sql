-- Payments service schema (spec section 9). This service owns these
-- tables exclusively; no other service reads or writes them.

create type payment_status as enum (
  'PENDING',
  'AUTHORIZED',
  'FAILED',
  'REFUND_PENDING',
  'REFUNDED'
);

create table payments (
  id uuid primary key,
  order_id uuid not null unique,
  currency text not null,
  amount_minor bigint not null check (amount_minor >= 0),
  status payment_status not null,
  provider_reference text null unique,
  version bigint not null default 1 check (version > 0),
  failure_code text null,
  created_at timestamptz not null,
  updated_at timestamptz not null
);

create type payment_operation_type as enum (
  'AUTHORIZE',
  'REFUND'
);

create type payment_operation_status as enum (
  'SUCCEEDED',
  'FAILED'
);

create table payment_operations (
  id uuid primary key,
  payment_id uuid not null references payments (id),
  operation_type payment_operation_type not null,
  idempotency_key text not null unique,
  status payment_operation_status not null,
  attempts int not null default 1 check (attempts > 0),
  created_at timestamptz not null,
  updated_at timestamptz not null
);
