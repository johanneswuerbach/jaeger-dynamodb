FROM golang:1.16.3 AS build
ARG GOARCH=amd64
WORKDIR /src
ADD . .
RUN GOOS=linux GOARCH=${GOARCH} CGO_ENABLED=0 go build -o dynamodb-plugin -v -ldflags '-extldflags "-static"'

FROM jaegertracing/all-in-one:1.25.0

COPY --from=build /src/dynamodb-plugin /go/bin
