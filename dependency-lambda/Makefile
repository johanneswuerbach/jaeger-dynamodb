IMAGE_NAME = jaeger-dependency-lambda

build:
	docker build -t $(IMAGE_NAME) -f Dockerfile ../
	docker run --rm -v $(CURDIR):/out $(IMAGE_NAME) /bin/cp -r /src/dependency-lambda/dependency-lambda.zip /out/
	@echo "Written $(CURDIR)/dependency-lambda.zip"
