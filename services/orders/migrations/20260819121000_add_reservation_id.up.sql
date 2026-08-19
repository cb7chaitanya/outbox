-- Orders' own record of the inventory reservation backing this order,
-- captured from `reservation_succeeded`'s payload (spec section 8) so the
-- compensation path (payment failed -> release inventory, section 12) can
-- build a `release_inventory` command without a cross-service join
-- (section 6 forbids one) or a second round trip to inventory.
alter table orders add column reservation_id uuid null;
