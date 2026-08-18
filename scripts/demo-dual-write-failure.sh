#!/usr/bin/env bash
# Runs both naive dual-write fault demonstrations from PROJECT_2_SPEC.md
# section 11 against a locally running Compose stack, and prints the
# invariant violations each one reproduces. Requires `make up` to already
# have Postgres and Redpanda healthy; this script builds and runs the
# `orders` binary itself (DELIVERY_MODE=naive, FAILURE_INJECTION_ENABLED=true)
# and tears it down on exit.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

for bin in jq curl docker cargo; do
  command -v "$bin" >/dev/null || {
    echo "error: '$bin' is required on PATH" >&2
    exit 1
  }
done

if [ ! -f .env ]; then
  echo "error: .env not found; run 'make setup' first" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source .env
set +a

export DELIVERY_MODE=naive
export FAILURE_INJECTION_ENABLED=true
export ENVIRONMENT=development
ORDERS_PORT="${ORDERS_PORT:-8081}"
BASE_URL="http://localhost:${ORDERS_PORT}"
TOKEN="${FAILURE_INJECTION_TOKEN}"

echo "==> checking Postgres and Redpanda are up (run 'make up' if not)..."
for svc in postgres redpanda; do
  if ! docker compose ps --status running --services 2>/dev/null | grep -qx "$svc"; then
    echo "error: $svc is not running; run 'make up' first" >&2
    exit 1
  fi
done

echo "==> building orders (this may take a moment on first run)..."
cargo build -p orders --quiet

echo "==> starting orders (DELIVERY_MODE=naive, FAILURE_INJECTION_ENABLED=true)..."
cargo run -p orders --quiet &
ORDERS_PID=$!
cleanup() {
  echo "==> stopping orders (pid $ORDERS_PID)"
  kill "$ORDERS_PID" >/dev/null 2>&1 || true
  wait "$ORDERS_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> waiting for /health/ready..."
ready=0
for _ in $(seq 1 40); do
  if curl -sf "$BASE_URL/health/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if [ "$ready" -ne 1 ]; then
  echo "error: orders never became ready" >&2
  exit 1
fi
echo "    orders is ready at $BASE_URL"

fail=0
tmp_dir=$(mktemp -d)
trap 'cleanup; rm -rf "$tmp_dir"' EXIT

matching_records() {
  # Bounded ~3s read of the whole topic (small dev topic; fine to re-read
  # from the start each time), filtered to one order's aggregate_id.
  # GNU `timeout` isn't present on macOS by default, so bound it manually:
  # background the consumer, let it run, then kill it and read what it
  # printed so far.
  local order_id="$1"
  local out="$tmp_dir/consume-$$-$RANDOM.json"
  docker compose exec -T redpanda \
    rpk topic consume orders.events.v1 -o start -n 0 --format json \
    >"$out" 2>/dev/null &
  local consume_pid=$!
  sleep 3
  kill "$consume_pid" >/dev/null 2>&1 || true
  wait "$consume_pid" 2>/dev/null || true
  jq -r --arg oid "$order_id" \
    'select((.value | fromjson | .aggregate_id) == $oid) | (.value | fromjson | .event_id)' \
    "$out" 2>/dev/null || true
}

echo
echo "=== Gap 1: orders.after_db_commit_before_publish ==="
echo "    (DB commit succeeds; the fault fires before the publish is attempted)"
curl -sf -X PUT "$BASE_URL/_test/faults/orders.after_db_commit_before_publish" \
  -H "content-type: application/json" -H "x-test-token: $TOKEN" \
  -d '{"fail_next": 1}' -o /dev/null

key1="demo-gap1-$(date +%s%N)"
status1=$(curl -s -o "$tmp_dir/gap1.json" -w '%{http_code}' -X POST "$BASE_URL/v1/orders" \
  -H "content-type: application/json" -H "idempotency-key: $key1" \
  -d '{"items":[{"sku":"SKU-DEMO","quantity":1,"unit_price_minor":500}],"currency":"USD"}')
echo "    create response: HTTP $status1"
jq . "$tmp_dir/gap1.json" 2>/dev/null | sed 's/^/    /' || cat "$tmp_dir/gap1.json"

order1_id=$(docker compose exec -T postgres \
  psql -U "$POSTGRES_USER" -d orders -tAc \
  "select id from orders where idempotency_key = '$key1'" | tr -d '[:space:]')

if [ "$status1" = "503" ] && [ -n "$order1_id" ]; then
  echo "    INVARIANT VIOLATION: order $order1_id is committed in Postgres"
  echo "    (idempotency_key=$key1) but the client received an injected"
  echo "    failure before the event was ever published."
else
  echo "    UNEXPECTED: status1=$status1 order1_id=${order1_id:-<none>}" >&2
  fail=1
fi

echo "    checking Kafka for an OrderCreated event for $order1_id (should find none)..."
matches1=$(matching_records "$order1_id")
if [ -z "$matches1" ]; then
  echo "    confirmed: zero orders.order_created events exist for $order1_id."
  echo "    No retry of any kind closes this gap — the DB and the broker were"
  echo "    never coordinated in the first place, so there is nothing left to retry."
else
  echo "    UNEXPECTED: found event(s) for $order1_id: $matches1" >&2
  fail=1
fi

echo
echo "=== Gap 2: orders.after_publish_before_response ==="
echo "    (DB commit and publish both succeed; the fault fires before the response)"
curl -sf -X PUT "$BASE_URL/_test/faults/orders.after_publish_before_response" \
  -H "content-type: application/json" -H "x-test-token: $TOKEN" \
  -d '{"fail_next": 1}' -o /dev/null

key2="demo-gap2-$(date +%s%N)"
body2='{"items":[{"sku":"SKU-DEMO","quantity":1,"unit_price_minor":500}],"currency":"USD"}'
status2a=$(curl -s -o "$tmp_dir/gap2a.json" -w '%{http_code}' -X POST "$BASE_URL/v1/orders" \
  -H "content-type: application/json" -H "idempotency-key: $key2" -d "$body2")
echo "    first attempt: HTTP $status2a (client sees a failure despite the publish having succeeded)"

status2b=$(curl -s -o "$tmp_dir/gap2b.json" -w '%{http_code}' -X POST "$BASE_URL/v1/orders" \
  -H "content-type: application/json" -H "idempotency-key: $key2" -d "$body2")
order2_id=$(jq -r '.id // empty' "$tmp_dir/gap2b.json")
echo "    naive client retry: HTTP $status2b, order_id=$order2_id"

if [ "$status2a" = "503" ] && [ "$status2b" = "202" ] && [ -n "$order2_id" ]; then
  matches2=$(matching_records "$order2_id")
  count2=$(printf '%s\n' "$matches2" | grep -c . || true)
  echo "    events observed for $order2_id: $count2"
  if [ "$count2" -ge 2 ]; then
    echo "    INVARIANT VIOLATION: order $order2_id has $count2 distinct"
    echo "    orders.order_created deliveries (event_ids: $(printf '%s' "$matches2" | tr '\n' ' '))."
    echo "    The idempotency-key layer correctly prevented a second order row,"
    echo "    but nothing coupled that decision to the naive publish step, so"
    echo "    the client's retry produced a real duplicate event on the broker."
  else
    echo "    UNEXPECTED: expected >=2 deliveries, saw $count2" >&2
    fail=1
  fi
else
  echo "    UNEXPECTED: status2a=$status2a status2b=$status2b order2_id=${order2_id:-<none>}" >&2
  fail=1
fi

curl -sf -X DELETE "$BASE_URL/_test/faults" -H "x-test-token: $TOKEN" -o /dev/null || true

echo
if [ "$fail" -eq 0 ]; then
  echo "=== Both dual-write gaps reproduced deterministically. ==="
  echo "See docs/failure-lab.md for why retries alone cannot close either one."
else
  echo "=== Demo did not reproduce the expected gaps; see output above. ===" >&2
fi
exit "$fail"
