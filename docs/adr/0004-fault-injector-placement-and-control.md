# ADR 0004: FaultInjector lives in test-support; controls are runtime-disabled by default

**Status:** accepted
**Date:** 2026-08-19
**Milestone:** M02

## Context

Spec section 17 requires named, deterministic fault points reached
through an injected `FaultInjector` port — never scattered sleeps or
panics in domain code — plus dev/test-only HTTP controls
(`PUT /_test/faults/{name}`, `DELETE /_test/faults`) gated by
`FAILURE_INJECTION_ENABLED=true`, refusing to enable in an environment
named `production`, and requiring a token. Two things needed a home:
where the `FaultInjector` type itself lives, and how a production build
avoids ever exposing the control surface.

## Decision

- **Crate placement:** `FaultInjector` (`crates/test-support/src/
  fault.rs`) lives in `test-support`, per spec section 7's repository
  table, which explicitly assigns "fault controls" to that crate.
  Services depend on `test-support` as a normal (non-dev) dependency,
  because the fault points are called from real request-handling code
  paths (`services/orders/src/http.rs`), not only from tests — the whole
  point is that a running service, not a test harness, is what gets
  faulted.
- **Runtime, not compile-time, disabling:** `AppState.fault_injector` is
  always constructed (an empty, unconfigured `FaultInjector` costs
  nothing at rest — `maybe_fail` on an unconfigured name is a single
  `HashMap::get` returning `None`). What's actually gated is the HTTP
  surface: `router()` only mounts `/_test/faults/*` when
  `failure_injection_enabled` is true (`services/orders/src/
  http.rs::router`). Spec section 17 allows either "compile or runtime-
  disable"; runtime-disable was chosen because a compile-time feature
  flag would mean the fault points themselves don't exist in a normal
  dev build either, making it impossible to run the demo script and the
  ordinary service from the same binary.
- **Startup refusal in production:** `FailureInjectionConfig::load()`
  (`services/orders/src/config.rs`) returns an error — which `main`
  propagates via `?`, failing process startup — if
  `FAILURE_INJECTION_ENABLED=true` and `ENVIRONMENT=production`. This is
  checked once at startup, not per-request, so there's no way to flip it
  on later via the control endpoints themselves (they only configure
  named faults, not the enablement flag).
- **Token check:** a single shared bearer-style token
  (`FAILURE_INJECTION_TOKEN`, compared via `X-Test-Token`) gates both
  control endpoints. Not a real auth system — the environment-name
  refusal is the actual safety net; the token exists so an
  accidentally-enabled dev instance reachable on a shared network isn't
  faultable by anyone who finds the port.

## Alternatives considered

- **Cargo feature flag compiling the fault points out entirely.** Rejected
  per above: would make `cargo run -p orders` (no features) behave
  differently from what the demo script and dual-write tests need, and
  would require a second build profile just for the failure lab.
- **A separate `fault-injector` crate.** Unnecessary — `test-support`
  already exists for exactly this per the spec's own crate table; a new
  crate would just add a workspace member with no distinct
  responsibility.

## Consequences

- Any future service (inventory, payments, fulfilment) that needs a
  fault point follows the same shape: depend on `test-support`, hold an
  `Arc<FaultInjector>` in its state, call `maybe_fail` at the named
  point, and conditionally mount its own `/_test/faults/*` routes.
- `FaultInjector::maybe_fail` takes an optional `subject` filter so a
  fault can target one in-flight order without affecting concurrent
  traffic sharing the same process — used by both dual-write
  demonstration tests to avoid cross-test interference when tests run
  concurrently against the same `#[sqlx::test]`-isolated database but a
  shared broker.
