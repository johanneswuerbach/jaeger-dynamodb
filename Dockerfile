FROM golang:1.17.3 AS base

FROM base AS code
ARG GOARCH=amd64
WORKDIR /src
ADD . .

FROM code AS build
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
  GOOS=linux GOARCH=${GOARCH} CGO_ENABLED=0 go build -o dynamodb-plugin -v -ldflags '-extldflags "-static"'

FROM code AS test

FROM base AS jaeger-grpc-integration
ARG GOARCH=amd64
RUN git clone --depth=1 --single-branch --branch=v1.28.0 https://github.com/jaegertracing/jaeger.git /jaeger
WORKDIR /jaeger
COPY --from=build /src/dynamodb-plugin /go/bin

FROM jaegertracing/all-in-one:1.28.0 AS jaeger-test
COPY --from=build /src/dynamodb-plugin /go/bin

FROM alpine:3.14.3

COPY --from=build /src/dynamodb-plugin /jaeger-dynamodb

# The /plugin is used by the jaeger-operator https://github.com/jaegertracing/jaeger-operator/pull/1517
CMD ["cp", "/jaeger-dynamodb", "/plugin/jaeger-dynamodb"]
