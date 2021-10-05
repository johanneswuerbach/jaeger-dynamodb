FROM golang:1.17.1 AS base

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
RUN git clone https://github.com/jaegertracing/jaeger.git /jaeger
WORKDIR /jaeger
# Checkout a commit instead of a version as the latest version v1.26.0
# is missing changes we need for the grpc integration tests
RUN git checkout b5d340dbc5a17ded4f291dbcb94ae62dbc3149ff
COPY --from=build /src/dynamodb-plugin /go/bin

FROM jaegertracing/all-in-one:1.26.0

COPY --from=build /src/dynamodb-plugin /go/bin
