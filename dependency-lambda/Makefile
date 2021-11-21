IMAGE_NAME = jaeger-dependency-lambda

build:
	DOCKER_BUILDKIT=1 docker build -t $(IMAGE_NAME) -f Dockerfile ../
	docker run --rm -v $(CURDIR):/out $(IMAGE_NAME) /bin/cp -r /src/dependency-lambda/dependency-lambda.zip /out/
	@echo "Written $(CURDIR)/dependency-lambda.zip"

test: build
	docker run --rm -e CGO_ENABLED=0 -i $(IMAGE_NAME) go test ./...
