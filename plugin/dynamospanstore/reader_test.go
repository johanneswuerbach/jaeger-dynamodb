package dynamospanstore

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/johanneswuerbach/jaeger-dynamodb/setup"
	"github.com/stretchr/testify/assert"
)

const (
	loggerName = "jaeger-dynamodb"
)

func TestGetServices(t *testing.T) {
	assert := assert.New(t)

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

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		lo.Credentials = credentials.NewStaticCredentialsProvider("TEST_ONLY", "TEST_ONLY", "TEST_ONLY")
		lo.Region = "us-east-1"
		lo.EndpointResolver = aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000", Source: aws.EndpointSourceCustom}, nil
			})
		return nil
	})
	assert.NoError(err)

	var (
		spansTable      = "jaeger.spans"
		servicesTable   = "jaeger.services"
		operationsTable = "jaeger.operations"
	)

	svc := dynamodb.NewFromConfig(cfg)
	reader := NewReader(logger, svc, spansTable, servicesTable, operationsTable)
	writer := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)

	assert.NoError(setup.RecreateTables(ctx, svc, &setup.SetupOptions{
		SpansTable:      spansTable,
		ServicesTable:   servicesTable,
		OperationsTable: operationsTable,
	}))

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(`{
		"traceId": "AAAAAAAAAAAAAAAAAAAAEQ==",
		"spanId": "AAAAAAAAAAM=",
		"operationName": "example-operation-1",
		"references": [],
		"startTime": "2017-01-26T16:46:31.639875Z",
		"duration": "100000ns",
		"tags": [],
		"process": {
			"serviceName": "example-service-1",
			"tags": []
		},
		"logs": [
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			},
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			}
		]
	}`), &span))
	assert.NoError(writer.WriteSpan(ctx, &span))

	serviceNames, err := reader.GetServices(ctx)
	assert.NoError(err)
	assert.ElementsMatch(serviceNames, []string{"example-service-1"})
}

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

	cfg, err := config.LoadDefaultConfig(ctx, config.WithClientLogMode(aws.LogRetries|aws.LogRequestWithBody|aws.LogResponseWithBody))
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

	startTimeMax := parseTime(t, "2021-09-06T21:51:20.839Z")
	startTimeMin := parseTime(t, "2021-09-06T20:51:20.839Z")

	traces, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
		ServiceName:  "frontend",
		StartTimeMin: startTimeMin,
		StartTimeMax: startTimeMax,
		NumTraces:    20,
		// Tags: map[string]string{
		// 	"driver": "T707765C",
		// },
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
