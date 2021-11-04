package dynamospanstore

import (
	"context"
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
	loggerName      = "jaeger-dynamodb"
	spansTable      = "jaeger.spans"
	servicesTable   = "jaeger.services"
	operationsTable = "jaeger.operations"
)

func createDynamoDBSvc(assert *assert.Assertions, ctx context.Context) *dynamodb.Client {
	dynamodbURL := os.Getenv("DYNAMODB_URL")
	if dynamodbURL == "" {
		dynamodbURL = "http://localhost:8000"
	}

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		lo.Credentials = credentials.NewStaticCredentialsProvider("TEST_ONLY", "TEST_ONLY", "TEST_ONLY")
		lo.Region = "us-east-1"
		lo.EndpointResolver = aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: dynamodbURL, Source: aws.EndpointSourceCustom}, nil
			})
		return nil
	})
	assert.NoError(err)

	return dynamodb.NewFromConfig(cfg)
}

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

	svc := createDynamoDBSvc(assert, ctx)
	reader := NewReader(logger, svc, spansTable, servicesTable, operationsTable)
	writer, err := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)
	assert.NoError(err)

	assert.NoError(setup.RecreateSpanStoreTables(ctx, svc, &setup.SetupSpanOptions{
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

const spanWithOperation = `{
	"traceId": "AAAAAAAAAAAAAAAAAAAAEQ==",
	"spanId": "AAAAAAAAAAM=",
	"operationName": "example-operation-1",
	"references": [],
	"startTime": "2017-01-26T16:46:31.639875Z",
	"duration": "100000ns",
	"tags": [],
	"process": {
		"serviceName": "query12-service",
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
}`

func TestGetOperations(t *testing.T) {
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

	svc := createDynamoDBSvc(assert, ctx)
	reader := NewReader(logger, svc, spansTable, servicesTable, operationsTable)
	writer, err := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)
	assert.NoError(err)

	assert.NoError(setup.RecreateSpanStoreTables(ctx, svc, &setup.SetupSpanOptions{
		SpansTable:      spansTable,
		ServicesTable:   servicesTable,
		OperationsTable: operationsTable,
	}))

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(spanWithOperation), &span))
	assert.NoError(writer.WriteSpan(ctx, &span))

	operations, err := reader.GetOperations(ctx, spanstore.OperationQueryParameters{ServiceName: "query12-service"})
	assert.NoError(err)
	assert.ElementsMatch(operations, []spanstore.Operation{{Name: "example-operation-1"}})
}

const inputWithTraceTag = `{
	"traceId": "AAAAAAAAAAAAAAAAAAAAEg==",
	"spanId": "AAAAAAAAAAQ=",
	"operationName": "query12-operation",
	"references": [],
	"tags": [
		{
			"key": "sameplacetag1",
			"vType": "STRING",
			"vStr": "sameplacevalue"
		}
	],
	"startTime": "2017-01-26T16:46:31.639875Z",
	"duration": "2000ns",
	"process": {
		"serviceName": "query12-service",
		"tags": []
	},
	"logs": []
}`

const inputWithLogTag = `{
	"traceId": "AAAAAAAAAAAAAAAAAAAAEg==",
	"spanId": "AAAAAAAAAAQ=",
	"operationName": "query12-operation",
	"references": [],
	"tags": [],
	"startTime": "2017-01-26T16:46:31.639875Z",
	"duration": "2000ns",
	"process": {
		"serviceName": "query12-service",
		"tags": []
	},
	"logs": [{
		"timestamp": "2017-01-26T16:46:31.639875Z",
		"fields": [
			{
				"key": "sameplacetag1",
				"vType": "STRING",
				"vStr": "sameplacevalue"
			}
		]
	}]
}`

const inputWithProcessTag = `{
	"traceId": "AAAAAAAAAAAAAAAAAAAAEg==",
	"spanId": "AAAAAAAAAAQ=",
	"operationName": "query12-operation",
	"references": [],
	"tags": [],
	"startTime": "2017-01-26T16:46:31.639875Z",
	"duration": "2000ns",
	"process": {
		"serviceName": "query12-service",
		"tags": [{
			"key": "sameplacetag1",
			"vType": "STRING",
			"vStr": "sameplacevalue"
		}]
	},
	"logs": []
}`

func TestFindTraces(t *testing.T) {
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

	svc := createDynamoDBSvc(assert, ctx)
	reader := NewReader(logger, svc, spansTable, servicesTable, operationsTable)

	assert.NoError(setup.RecreateSpanStoreTables(ctx, svc, &setup.SetupSpanOptions{
		SpansTable:      spansTable,
		ServicesTable:   servicesTable,
		OperationsTable: operationsTable,
	}))

	startTimeMax := parseTime(t, "2017-01-26T16:50:31.639875Z")
	startTimeMin := parseTime(t, "2017-01-26T16:40:31.639875Z")

	type test struct {
		input string
	}

	tests := []test{
		{input: inputWithTraceTag},
		{input: inputWithLogTag},
		{input: inputWithProcessTag},
	}

	for _, tc := range tests {
		writer, err := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)
		assert.NoError(err)

		var span model.Span
		assert.NoError(jsonpb.Unmarshal(strings.NewReader(tc.input), &span))
		assert.NoError(writer.WriteSpan(ctx, &span))

		traces, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
			ServiceName:  "query12-service",
			StartTimeMin: startTimeMin,
			StartTimeMax: startTimeMax,
			NumTraces:    20,
			Tags: map[string]string{
				"sameplacetag1": "sameplacevalue",
			},
		})
		assert.NoError(err)
		assert.Len(traces, 1)
	}
}

func TestFindTracesWithLimit(t *testing.T) {
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

	svc := createDynamoDBSvc(assert, ctx)
	reader := NewReader(logger, svc, spansTable, servicesTable, operationsTable)
	writer, err := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)
	assert.NoError(err)

	assert.NoError(setup.RecreateSpanStoreTables(ctx, svc, &setup.SetupSpanOptions{
		SpansTable:      spansTable,
		ServicesTable:   servicesTable,
		OperationsTable: operationsTable,
	}))

	startTimeMax := parseTime(t, "2017-01-26T16:50:31.639875Z")
	startTimeMin := parseTime(t, "2017-01-26T16:40:31.639875Z")

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(inputWithTraceTag), &span))
	assert.NoError(writer.WriteSpan(ctx, &span))
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(spanWithOperation), &span))
	assert.NoError(writer.WriteSpan(ctx, &span))

	traces, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
		ServiceName:  "query12-service",
		StartTimeMin: startTimeMin,
		StartTimeMax: startTimeMax,
		NumTraces:    2,
	})
	assert.NoError(err)
	assert.Len(traces, 2)

	tracesSubset, err := reader.FindTraces(ctx, &spanstore.TraceQueryParameters{
		ServiceName:  "query12-service",
		StartTimeMin: startTimeMin,
		StartTimeMax: startTimeMax,
		NumTraces:    1,
	})
	assert.NoError(err)
	assert.Len(tracesSubset, 1)
}

func parseTime(t *testing.T, timeStr string) time.Time {
	time, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		t.Fatalf("failed to parse time %s, %v", timeStr, err)
	}

	return time
}
