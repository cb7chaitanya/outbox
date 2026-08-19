-- Inventory service schema (spec section 9). This service owns these
-- tables exclusively; no other service reads or writes them.

create table stock (
  sku text primary key,
  available_qty bigint not null check (available_qty >= 0),
  reserved_qty bigint not null check (reserved_qty >= 0),
  fulfilled_qty bigint not null check (fulfilled_qty >= 0),
  version bigint not null default 1 check (version > 0),
  created_at timestamptz not null,
  updated_at timestamptz not null
);

create type reservation_status as enum (
  'ACTIVE',
  'RELEASED',
  'COMMITTED',
  'REJECTED'
);

create table reservations (
  id uuid primary key,
  order_id uuid not null unique,
  status reservation_status not null,
  reason_code text null,
  version bigint not null default 1 check (version > 0),
  created_at timestamptz not null,
  updated_at timestamptz not null
);

create table reservation_items (
  reservation_id uuid not null references reservations (id),
  sku text not null,
  quantity bigint not null check (quantity > 0),
  primary key (reservation_id, sku)
);
