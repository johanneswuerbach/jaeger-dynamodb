# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)


ARCH := $(shell uname -m)
ifeq ($(ARCH),arm64)
	GOARCH = arm64
else ifeq ($(ARCH),x86_64)
	GOARCH = amd64
else
  GOARCH = $(shell go env GOARCH)
endif

build: ## Start build the jaeger image including the plugin
	docker compose build --build-arg GOARCH=$(GOARCH) jaeger

start: ## Start the hotrod demo and jaeger
	echo "Open http://0.0.0.0:8080"
	docker compose up hotrod

logs: ## Print the jaeger service logs
	docker compose logs -f jaeger

down: ## Shutdown all containers
	docker compose down -v

.PHONY: test
test: ## Run jaeger plugin tests
	docker compose build --build-arg GOARCH=$(GOARCH) test
	docker compose run --rm test go test -v ./...

test-jaeger-grpc-integration: ## Run jaeger integration tests for grpc plugins
	docker compose up -d dynamodb
	docker compose build --build-arg GOARCH=$(GOARCH) test-jaeger-grpc-integration
	docker compose run --rm test-jaeger-grpc-integration sh -c '$$PLUGIN_BINARY_PATH --config $$PLUGIN_CONFIG_PATH --create-tables=1 --only-create-tables=true'
	docker compose run --rm test-jaeger-grpc-integration go test -run 'TestGRPCStorage/FindTraces' -v -race ./plugin/storage/integration/...

lint: ## Lint the code
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.42.1 golangci-lint run -v

build-dependency-lambda: ## Build the dependency lambda
	make -C ./dependency-lambda build
