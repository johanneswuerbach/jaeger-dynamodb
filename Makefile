ARCH := $(shell uname -m)
ifeq ($(ARCH),arm64)
	GOARCH = arm64
else ifeq ($(ARCH),x86_64)
	GOARCH = amd64
else
  GOARCH = $(shell go env GOARCH)
endif


build:
	docker-compose build --build-arg GOARCH=$(GOARCH) jaeger

start:
	echo "Open http://0.0.0.0:8080"
	docker-compose up hotrod

logs:
	docker-compose logs -f jaeger
