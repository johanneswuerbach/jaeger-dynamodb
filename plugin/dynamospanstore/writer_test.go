package dynamospanstore

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

type mockPutItemAPI func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)

func (m mockPutItemAPI) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m(ctx, params, optFns...)
}

func TestWriteSpanDedupe(t *testing.T) {
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

	var (
		spansTable      = "jaeger.spans"
		servicesTable   = "jaeger.services"
		operationsTable = "jaeger.operations"
	)

	writesPerTable := map[string]int{
		spansTable:      0,
		servicesTable:   0,
		operationsTable: 0,
	}

	var mu sync.Mutex
	svc := mockPutItemAPI(func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
		mu.Lock()
		writesPerTable[*params.TableName] += 1
		mu.Unlock()

		return nil, nil
	})

	writer, err := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)
	assert.NoError(err)

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
	assert.NoError(writer.WriteSpan(ctx, &span))

	assert.Equal(writesPerTable[spansTable], 2)
	assert.Equal(writesPerTable[servicesTable], 1)
	assert.Equal(writesPerTable[operationsTable], 1)
}
