DEV_DSN ?= cdc:cdc@tcp(127.0.0.1:13306)/
DEV_META_DSN ?= cdc:cdc@tcp(127.0.0.1:13306)/changeflow_meta?parseTime=true
DEV_SHOP_DSN ?= cdc:cdc@tcp(127.0.0.1:13306)/shop?parseTime=true
# Mutations run as a user permitted to write; the replication user must not be.
DEV_WRITE_DSN ?= root:root@tcp(127.0.0.1:13306)/shop?parseTime=true
COMPOSE := docker compose -f deploy/dev/docker-compose.yml

.PHONY: help build test test-integration test-e2e bench budgets vet check check-all dev-up dev-sinks dev-down dev-logs dev-mysql dev-es dev-ch preflight tail

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary
	go build -o bin/changeflow ./cmd/changeflow

test: ## Run unit tests
	go test ./... -count=1

test-integration: ## Run tests that need the dev containers running
	CHANGEFLOW_TEST_DSN="$(DEV_SHOP_DSN)" \
	CHANGEFLOW_TEST_META_DSN="$(DEV_META_DSN)" \
	CHANGEFLOW_TEST_WRITE_DSN="$(DEV_WRITE_DSN)" \
	go test ./... -count=1

test-e2e: ## Run end-to-end tests against MySQL and Elasticsearch (needs make dev-sinks)
	CHANGEFLOW_E2E=1 \
	CHANGEFLOW_E2E_MYSQL_DSN="root:root@tcp(127.0.0.1:13306)/shop" \
	CHANGEFLOW_E2E_META_DSN="root:root@tcp(127.0.0.1:13306)/changeflow_meta" \
	CHANGEFLOW_E2E_ES_URL="http://127.0.0.1:19200" \
	go test ./test/e2e/ -count=1 -v -p 1 -timeout 20m

bench: ## Run benchmarks
	go test ./... -run XXX -bench . -benchmem

budgets: ## Assert the performance budgets the design commits to
	go test ./internal/pipeline/ ./internal/sink/elasticsearch/ -count=1 -run 'Budget|Cheap' -v

vet: ## Static checks
	go vet ./...

check: vet test budgets ## Fast checks, no containers needed

check-all: vet test-integration ## Everything, including tests against live services

dev-up: ## Start a CDC-configured MySQL on port 13306
	$(COMPOSE) up -d --wait

dev-sinks: ## Also start Elasticsearch and ClickHouse (needs ~3 GB free disk)
	$(COMPOSE) --profile sinks up -d --wait

dev-down: ## Stop everything and delete its data
	$(COMPOSE) --profile sinks down -v

dev-logs: ## Tail MySQL logs
	$(COMPOSE) logs -f mysql

dev-mysql: ## Open a mysql shell as root
	$(COMPOSE) exec mysql mysql -uroot -proot shop

dev-es: ## Show Elasticsearch health
	curl -s http://127.0.0.1:19200/_cluster/health | python3 -m json.tool

dev-ch: ## Open a ClickHouse shell
	$(COMPOSE) exec clickhouse clickhouse-client --user changeflow --password changeflow

preflight: ## Check the dev MySQL is configured for CDC
	go run ./cmd/changeflow preflight --dsn "$(DEV_DSN)"

tail: ## Tail shop.orders and capture fixtures
	go run ./cmd/changeflow tail --dsn "$(DEV_DSN)" \
		--table shop.orders --table shop.order_items \
		--capture test/fixtures/binlog --for 60s
