#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
set -a
. ./.env
set +a

tmp_dir=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  docker compose start postgres redpanda >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

eventually() {
  local description=$1
  shift
  local deadline=$((SECONDS + 30))
  until "$@"; do
    if (( SECONDS >= deadline )); then
      echo "timeout: $description" >&2
      return 1
    fi
    sleep 0.2
  done
}

start_service() {
  local service=$1
  FAILURE_INJECTION_ENABLED=true cargo run -p "$service" >"$tmp_dir/$service.log" 2>&1 &
  pids+=("$!")
}

for service in orders inventory payments fulfilment; do start_service "$service"; done
for port in 8081 8082 8083 8084; do
  eventually "service $port ready" curl -fsS "http://localhost:$port/health/ready"
done

docker compose exec -T postgres psql -U "$POSTGRES_USER" -d inventory -v ON_ERROR_STOP=1 \
  -c "insert into stock (sku, available_qty, reserved_qty, fulfilled_qty, version, created_at, updated_at) values ('SKU-CHAOS',100,0,0,1,now(),now()) on conflict (sku) do update set available_qty=100,reserved_qty=0,fulfilled_qty=0,version=stock.version+1,updated_at=now()" >/dev/null

# Broker outage: HTTP acceptance and DB+outbox commit continue, then drain.
docker compose stop redpanda >/dev/null
key="chaos-broker-$(date +%s)"
curl -fsS -X POST http://localhost:8081/v1/orders -H 'content-type: application/json' \
  -H "idempotency-key: $key" --data '{"items":[{"sku":"SKU-CHAOS","quantity":1,"unit_price_minor":100}],"currency":"USD"}' \
  >"$tmp_dir/order.json"
order_id=$(jq -r .id "$tmp_dir/order.json")
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d orders -Atc \
  "select count(*) > 0 from outbox_events where aggregate_id='$order_id' and published_at is null" | grep -qx t
docker compose start redpanda >/dev/null
eventually "redpanda healthy" docker compose exec -T redpanda rpk cluster health --exit-when-healthy
eventually "order completes after broker recovery" bash -c \
  "curl -fsS http://localhost:8081/v1/orders/$order_id | jq -e '.status == \"COMPLETED\"' >/dev/null"

# DB outage: readiness fails, restart restores it without wiping volumes.
docker compose stop postgres >/dev/null
eventually "orders reports not ready" bash -c \
  "test \"\$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/health/ready)\" = 503"
docker compose start postgres >/dev/null
eventually "postgres healthy" docker compose exec -T postgres pg_isready -U "$POSTGRES_USER"
eventually "orders readiness recovers" curl -fsS http://localhost:8081/health/ready

# Worker kill/restart: kill orders and restart it against retained offsets.
kill "${pids[0]}"
wait "${pids[0]}" 2>/dev/null || true
start_service orders
eventually "orders restart ready" curl -fsS http://localhost:8081/health/ready

# Poison followed by valid work: malformed record goes to DLQ and cannot block.
printf 'not-json' | docker compose exec -T redpanda rpk topic produce inventory.commands.v1 \
  --brokers localhost:9092 -k poison >/dev/null
key="chaos-poison-$(date +%s)"
curl -fsS -X POST http://localhost:8081/v1/orders -H 'content-type: application/json' \
  -H "idempotency-key: $key" --data '{"items":[{"sku":"SKU-CHAOS","quantity":1,"unit_price_minor":100}],"currency":"USD"}' \
  >"$tmp_dir/poison-order.json"
poison_order=$(jq -r .id "$tmp_dir/poison-order.json")
correlation_id=$(jq -r .correlation_id "$tmp_dir/poison-order.json")
eventually "valid work progresses after poison" bash -c \
  "curl -fsS http://localhost:8081/v1/orders/$poison_order | jq -e '.status == \"COMPLETED\"' >/dev/null"
for service in orders inventory payments fulfilment; do
  eventually "$service correlation log" grep -q "$correlation_id" "$tmp_dir/$service.log"
done
for port in 8081 8082 8083 8084; do
  curl -fsS "http://localhost:$port/metrics" | grep -q consumer_lag_records
done

echo "chaos smoke passed: recovery, poison isolation, correlated logs, metrics"
