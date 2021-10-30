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

type writeMock struct {
	writesPerTable map[string]int
	mu             sync.Mutex
}

func NewWriteMock() *writeMock {
	return &writeMock{
		writesPerTable: map[string]int{
			spansTable:      0,
			servicesTable:   0,
			operationsTable: 0,
		},
	}
}

func (m *writeMock) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	m.writesPerTable[*params.TableName] += 1
	m.mu.Unlock()

	return nil, nil
}
func (m *writeMock) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	m.mu.Lock()
	for tableName, items := range params.RequestItems {
		m.writesPerTable[tableName] += len(items)
	}
	m.mu.Unlock()

	return nil, nil
}

const (
	exampleSpan = `{
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
	}`
)

func createWrite(assert *assert.Assertions, svc DynamoDBAPI) *Writer {
	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Warn.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	writer, err := NewWriter(logger, svc, spansTable, servicesTable, operationsTable)
	assert.NoError(err)
	return writer
}

func TestWriteSpanDedupe(t *testing.T) {
	assert := assert.New(t)

	ctx := context.TODO()

	svc := NewWriteMock()

	writer := createWrite(assert, svc)

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(exampleSpan), &span))
	assert.NoError(writer.WriteSpan(ctx, &span))
	assert.NoError(writer.WriteSpan(ctx, &span))
	assert.NoError(writer.Close())

	assert.Equal(2, svc.writesPerTable[spansTable])
	assert.Equal(1, svc.writesPerTable[servicesTable])
	assert.Equal(1, svc.writesPerTable[operationsTable])
}

func BenchmarkWrite(b *testing.B) {
	assert := assert.New(b)
	ctx := context.TODO()

	svc := createDynamoDBSvc(assert, ctx)
	writer := createWrite(assert, svc)

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(exampleSpan), &span))

	for n := 0; n < b.N; n++ {
		assert.NoError(writer.WriteSpan(ctx, &span))
	}
}
