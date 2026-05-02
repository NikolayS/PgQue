SHELL := /usr/bin/env bash
DURATION ?= 300
PGURI ?= postgresql://bench:bench@127.0.0.1:55435/bench

.PHONY: up down setup-skiplocked setup-pgque \
        run-s1 run-s2 run-all clean report

up:
	docker compose up -d
	@for i in $$(seq 1 30); do \
	  docker exec pgque-bench-pg pg_isready -U bench -d bench >/dev/null 2>&1 && exit 0; \
	  sleep 1; \
	done; \
	echo "PG did not become ready"; exit 1

down:
	docker compose down -v

setup-skiplocked:
	psql "$(PGURI)" -v ON_ERROR_STOP=1 -f sql/setup-common.sql
	psql "$(PGURI)" -v ON_ERROR_STOP=1 -f sql/setup-skiplocked.sql

setup-pgque:
	psql "$(PGURI)" -v ON_ERROR_STOP=1 -f sql/setup-skiplocked.sql
	docker exec -i pgque-bench-pg psql -U bench -d bench -v ON_ERROR_STOP=1 -f /dev/stdin <sql/setup-pgque.sql

run-s1: up
	$(MAKE) setup-skiplocked
	bash scripts/run-scenario.sh skiplocked s1 $(DURATION)
	$(MAKE) setup-pgque
	bash scripts/run-scenario.sh pgque s1 $(DURATION)

run-s2: up
	$(MAKE) setup-skiplocked
	bash scripts/run-scenario.sh skiplocked s2 $(DURATION)
	$(MAKE) setup-pgque
	bash scripts/run-scenario.sh pgque s2 $(DURATION)

run-all: run-s1 run-s2 report

report:
	bash scripts/report.sh

clean:
	rm -rf results/
