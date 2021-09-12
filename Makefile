ARCH := $(shell uname -m)
ifeq ($(ARCH),arm64)
	GOARCH = arm64
else ifeq ($(ARCH),x86_64)
	GOARCH = amd64
else
  GOARCH = $(shell go env GOARCH)
endif

build:
	docker compose build --build-arg GOARCH=$(GOARCH) jaeger

start:
	echo "Open http://0.0.0.0:8080"
	docker compose up hotrod

logs:
	docker compose logs -f jaeger

down:
	docker compose down -v

test-integration:
	docker compose build --build-arg GOARCH=$(GOARCH) test-integration
	docker compose run --rm test-integration sh -c '$$PLUGIN_BINARY_PATH --config $$PLUGIN_CONFIG_PATH --create-tables=1 --only-create-tables=true'
	docker compose run --rm test-integration go test -run 'TestGRPCStorage/FindTraces' -v -race ./plugin/storage/integration/...
