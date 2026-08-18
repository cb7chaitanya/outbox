-- Correlation ID is accepted or generated on order creation (spec section
-- 10) and persisted so later milestones can attach it to every event this
-- order produces (spec section 16).
alter table orders add column correlation_id uuid not null default gen_random_uuid();
alter table orders alter column correlation_id drop default;
