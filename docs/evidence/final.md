# Final acceptance evidence (M10)

Date: 2026-08-19 (Asia/Kolkata)

## Environment and clean start

- `rustc 1.94.0 (4a4ef493e 2026-03-02)`
- `cargo 1.94.0 (85eff7c80 2026-01-15)`
- Docker Compose `v2.31.0-desktop.2`
- `make reset CONFIRM=yes && make up`: passed from empty project volumes.
- The migration job applied all four services' migrations, auto topic creation
  was disabled, all workflow/DLQ topics were created explicitly, and all four
  service health checks passed.

## Repeated gates

| Command | Result |
|---|---|
| `PROPTEST_RNG_SEED=2026081901 make test` | passed after fixing live-consumer test isolation |
| `PROPTEST_RNG_SEED=2026081902 make test` | passed |
| `cargo test --workspace --doc` | passed |
| `docker compose --profile observability config -q` | passed |
| `make demo-naive-failure` | passed; reproduced lost event and duplicate publish windows |
| `make chaos-smoke` | passed; broker/DB recovery, worker restarts, poison isolation, logs, metrics |

The first attempt correctly exposed that Compose consumers could race test
consumers on shared topics. Commit `15b45f0` makes `make test-integration`
pause only running application consumers and restore them with a shell trap.
Both recorded passes above use that deterministic arrangement.

## Required deterministic scenarios

1. Happy path: `happy_path_reaches_fulfilment_readiness` plus
   `fulfilment_success_completes_exactly_once`.
2. Insufficient stock: `inventory_failure_cancels_with_no_payment_operation`.
3. Decline/release: `payment_failure_releases_inventory_then_cancels`.
4. Fulfilment compensation: `fulfilment_failure_waits_for_both_compensations`.
5. Duplicate/reorder: `duplicated_and_reordered_outcomes_do_not_create_illegal_transitions`.
6. Consume-commit crash: inventory `crash_after_db_commit_before_offset_commit_has_no_duplicate_effect`.
7. Publish-mark crash: `crash_after_publish_before_mark_causes_duplicate_then_eventual_published_mark`.
8. Concurrent idempotency: HTTP and repository concurrent-key tests.
9. No oversell: `concurrent_reservations_never_oversell`.
10. Stale protection: payments `stale_command_is_counted_and_has_no_second_effect`.
11. Version gaps: `recoverable_gap_applies_in_version_order` and contiguous-offset property test.
12. Broker outage: outbox backlog/drain test and chaos smoke.
13. DB outage: chaos readiness/recovery; no transaction can advance its offset before commit.
14. Poison isolation: inventory/payments poison tests and chaos smoke.
15. Replay singularity: duplicate inbox/provider/fulfilment tests; selected DLQ replay below.
16. Exhaustion: `exhausted_compensation_enters_manual_review_and_signals_dlq` and alert rule.

All scenario assertions use bounded polling or deterministic fakes; there are
no unexplained correctness sleeps.

## Replay, telemetry, and restart

`replay-dlq inventory.commands.v1 0 localhost:19092` replayed unsupported-schema
event `01a01a54-974d-7520-8cfc-2814a8179563` with its original identity and
`replay_count=1`. It returned to DLQ rather than creating a business effect,
as expected. Normal/duplicate replay tests prove singular reservation,
authorization, refund, and fulfilment effects.

Chaos smoke found one correlation ID in JSON logs from all four services and
queried their fixed-label recovery metrics. After `docker compose restart`
without volume deletion, PostgreSQL and Redpanda recovered and every service
returned readiness; orders metrics reported zero unpublished backlog.

The outbox equivalent is covered by
`outbox_mode_closes_the_naive_lost_event_window` and
`broker_outage_grows_backlog_then_drains_on_recovery`: accepted state and its
logical event commit together, and retained backlog drains after recovery.

## Final audit

- No ignored tests, required-scope TODO/FIXME, placeholder, or undocumented
  manual database edit remains.
- `.env`, credentials, target output, volumes, and raw logs are not committed.
- M00–M10 are complete. M11 remains explicitly optional and unimplemented.
