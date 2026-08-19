-- Per-(order, downstream target) command-version counter (spec section 14
-- ordering policy, M06 fix). Each downstream consumer (inventory, payments)
-- tracks a gapless per-aggregate version sequence independently, scoped to
-- what *that consumer* actually receives -- not the order's own global
-- version. M05 shipped a fixed `aggregate_version: 1` for
-- `authorize_payment`, correct only because it was the sole command orders
-- ever sent to payments; M06 adds `release_inventory` as a second command
-- on the orders->inventory relationship, so a real counter replaces the
-- constant. See docs/adr/0011-per-target-command-version-counter.md.
create table outbound_command_sequences (
  order_id uuid not null,
  target text not null,
  next_version bigint not null,
  primary key (order_id, target)
);
