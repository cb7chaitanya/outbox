.DEFAULT_GOAL := help
SHELL := /bin/bash

TOPICS := orders.events.v1 \
          inventory.commands.v1 inventory.events.v1 \
          payments.commands.v1 payments.events.v1 \
          fulfilment.commands.v1 fulfilment.events.v1

.PHONY: help setup up down reset migrate topics fmt lint \
        test-unit test-integration test-e2e test \
        demo-naive-failure chaos-smoke logs

help:
	@echo "See PROJECT_2_SPEC.md section 19 for the full command contract."

## Validate required tools are installed and describe env setup.
setup:
	@command -v cargo >/dev/null || (echo "cargo not found; install rustup" && exit 1)
	@command -v docker >/dev/null || (echo "docker not found" && exit 1)
	@docker compose version >/dev/null || (echo "docker compose plugin not found" && exit 1)
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")
	@cargo build --workspace
	@echo "setup complete"

## Bring up infrastructure and wait for readiness.
up:
	docker compose up -d postgres redpanda redpanda-console
	@echo "waiting for postgres and redpanda healthchecks..."
	@until [ "$$(docker compose ps -q postgres | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do sleep 1; done
	@until [ "$$(docker compose ps -q redpanda | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do sleep 1; done
	docker compose exec -T redpanda rpk cluster config set auto_create_topics_enabled false
	$(MAKE) topics
	docker compose up -d --build orders inventory payments fulfilment
	@for service in orders inventory payments fulfilment; do \
		until [ "$$(docker compose ps -q $$service | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do sleep 1; done; \
	done
	@echo "infrastructure and services healthy"

## Stop services without deleting data.
down:
	docker compose down

## Destructive local-only data reset. Requires CONFIRM=yes.
reset:
	@test "$(CONFIRM)" = "yes" || (echo "refusing to reset without CONFIRM=yes" && exit 1)
	docker compose down -v

## Apply database migrations for every service.
migrate:
	docker compose run --rm migrate

## Explicitly create the Kafka-compatible topics used by the workflow.
topics:
	@for t in $(TOPICS) $(addsuffix .dlq,$(TOPICS)); do \
		echo "creating topic $$t"; \
		docker compose exec -T redpanda rpk topic create $$t --brokers localhost:9092 || true; \
	done
	docker compose exec -T redpanda rpk topic list --brokers localhost:9092

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test-unit:
	cargo test --workspace --lib --bins

## Runs integration tests (tests/*.rs) against a real Postgres. Each
## #[sqlx::test] gets its own ephemeral, migrated database, so DATABASE_URL
## only needs to name a server the migrating user can create databases on.
test-integration:
	@test -f .env || (echo ".env not found; run 'make setup' first" && exit 1)
	@set -a && . ./.env && set +a && \
	DATABASE_URL="postgres://$$POSTGRES_USER:$$POSTGRES_PASSWORD@$$POSTGRES_HOST:$$POSTGRES_PORT/$$POSTGRES_ORDERS_DB" \
	cargo test --workspace --tests

test-e2e:
	@set -a && . ./.env && set +a && \
	DATABASE_URL="postgres://$$POSTGRES_USER:$$POSTGRES_PASSWORD@$$POSTGRES_HOST:$$POSTGRES_PORT/$$POSTGRES_ORDERS_DB" \
	cargo test -p orders --test choreography_tests

## Full required suite: format check, lint, and everything currently implemented.
test: fmt lint test-unit test-integration

demo-naive-failure:
	./scripts/demo-dual-write-failure.sh

chaos-smoke:
	./scripts/chaos-smoke.sh

## Tail logs for a specific order across services. Usage: make logs ORDER_ID=<uuid>
logs:
	@test -n "$(ORDER_ID)" || (echo "usage: make logs ORDER_ID=<uuid>" && exit 1)
	docker compose logs -f | grep --line-buffered "$(ORDER_ID)"
