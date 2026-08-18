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
	docker compose up -d
	@echo "waiting for postgres and redpanda healthchecks..."
	@until [ "$$(docker compose ps -q postgres | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do sleep 1; done
	@until [ "$$(docker compose ps -q redpanda | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do sleep 1; done
	docker compose exec -T redpanda rpk cluster config set auto_create_topics_enabled false
	@echo "infrastructure healthy"

## Stop services without deleting data.
down:
	docker compose down

## Destructive local-only data reset. Requires CONFIRM=yes.
reset:
	@test "$(CONFIRM)" = "yes" || (echo "refusing to reset without CONFIRM=yes" && exit 1)
	docker compose down -v

## Apply database migrations for every service.
migrate:
	@echo "migrate: not yet implemented (lands with M01 orders migrations)"

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

test-integration:
	@echo "test-integration: not yet implemented (lands with M03/M04 outbox+inbox work)"

test-e2e:
	@echo "test-e2e: not yet implemented (lands with M06/M07 choreographed workflow)"

## Full required suite: format check, lint, and everything currently implemented.
test: fmt lint test-unit

demo-naive-failure:
	@echo "demo-naive-failure: not yet implemented (lands with M02 dual-write failure lab)"

chaos-smoke:
	@echo "chaos-smoke: not yet implemented (lands with M09 observability and chaos)"

## Tail logs for a specific order across services. Usage: make logs ORDER_ID=<uuid>
logs:
	@test -n "$(ORDER_ID)" || (echo "usage: make logs ORDER_ID=<uuid>" && exit 1)
	docker compose logs -f | grep --line-buffered "$(ORDER_ID)"
