# ADR 0012: Explicit compensation exhaustion outcomes

- Status: Accepted
- Date: 2026-08-19

## Context

Orders owns saga convergence, but it cannot infer that a downstream
compensation has permanently exhausted its retry budget from a downstream
DLQ that it does not consume. Silence is ambiguous and would leave an order
in `CANCELLING` forever.

## Decision

Downstream services emit explicit terminal failure outcomes when a
compensation retry budget is exhausted: `payments.refund_failed` and
`inventory.release_failed`. Orders consumes these outcomes, transitions the
order from `CANCELLING` to `MANUAL_REVIEW`, publishes the source event to the
consumer DLQ with `COMPENSATION_EXHAUSTED`, and logs an operator signal.

Refund attempts use the same bounded full-jitter policy as authorization and
the provider's refund idempotency key. A later replay therefore cannot cause a
second real refund.

## Consequences

- Retry exhaustion is observable and converges intentionally instead of
  stranding an order.
- The event catalog contains two M07 extensions beyond the original section 8
  list; both are versioned and documented in `contracts`.
- Operators can distinguish business cancellation from compensation failure.
