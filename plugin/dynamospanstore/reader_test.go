package dynamospanstore

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	loggerName = "jaeger-dynamodb"
)

func TestFindTraces(t *testing.T) {
	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Warn.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	var (
		spansTable      = "jaeger.spans"
		servicesTable   = "jaeger.services"
		operationsTable = "jaeger.operations"
	)

	svc := dynamodb.NewFromConfig(cfg)
	reader := NewReader(logger, svc, spansTable, servicesTable, operationsTable)

	startTimeMax := parseTime(t, "2021-09-06T09:48:53.142290222Z")
	startTimeMin := parseTime(t, "2021-09-04T09:48:53.142289847Z")

	traces, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
		ServiceName:  "frontend",
		StartTimeMin: startTimeMin,
		StartTimeMax: startTimeMax,
	})
	if err != nil {
		t.Fatalf("failed to FindTraces, %v", err)
	}
	fmt.Println(traces)
}

func parseTime(t *testing.T, timeStr string) time.Time {
	time, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		t.Fatalf("failed to parse time %s, %v", timeStr, err)
	}

	return time
}
