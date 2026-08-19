alter table orders
  add column payment_id uuid null,
  add column fulfilment_id uuid null,
  add column compensation_release_required boolean not null default false,
  add column compensation_release_done boolean not null default false,
  add column compensation_refund_required boolean not null default false,
  add column compensation_refund_done boolean not null default false;
