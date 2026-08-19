alter table orders
  drop column compensation_refund_done,
  drop column compensation_refund_required,
  drop column compensation_release_done,
  drop column compensation_release_required,
  drop column fulfilment_id,
  drop column payment_id;
