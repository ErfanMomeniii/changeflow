DEV_DSN ?= cdc:cdc@tcp(127.0.0.1:13306)/
COMPOSE := docker compose -f deploy/dev/docker-compose.yml

.PHONY: help build test vet check dev-up dev-sinks dev-down dev-logs dev-mysql dev-es dev-ch preflight tail

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary
	go build -o bin/changeflow ./cmd/changeflow

test: ## Run unit tests
	go test ./... -count=1

vet: ## Static checks
	go vet ./...

check: vet test ## Everything CI runs

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
