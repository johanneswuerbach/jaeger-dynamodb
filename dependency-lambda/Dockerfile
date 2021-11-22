FROM golang:1.17.3-alpine AS base

ARG GOARCH=arm64

RUN apk add --no-cache zip

WORKDIR /src

ADD . .

WORKDIR /src/dependency-lambda

RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
  GOOS=linux GOARCH=${GOARCH} CGO_ENABLED=0 go build -o bootstrap -v -ldflags '-extldflags "-static"'

RUN zip dependency-lambda.zip bootstrap
