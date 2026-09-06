SHELL := /bin/sh
CONFIG ?= configs/pilot/order_v5_c2_f8.yaml

.PHONY: build up down reset-experiment run run-pilot run-matrix test

build:
	docker-compose build

up:
	docker-compose up -d db order-service payment-service inventory-service notification-service orchestrator tool-gateway

down:
	docker-compose down

reset-experiment:
	docker-compose exec -T db psql -U exactlyonce -d exactlyonce -f /migrations/reset_experiment.sql

run:
	PYTHONPATH=agent-simulator:fault-injector:experiment-runner:analysis python3 -m experiment_runner.run --config $(CONFIG)

run-pilot:
	PYTHONPATH=agent-simulator:fault-injector:experiment-runner:analysis python3 -m experiment_runner.pilot --config-dir configs/pilot

run-matrix:
	PYTHONPATH=agent-simulator:fault-injector:experiment-runner:analysis python3 -m experiment_runner.matrix --config-dir configs/full

test:
	PYTHONPATH=agent-simulator:fault-injector:experiment-runner:analysis python3 -m pytest acceptance-tests
