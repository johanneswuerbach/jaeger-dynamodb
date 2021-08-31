package plugin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamodependencystore"
	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamospanstore"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func NewDynamoDBPlugin(logger hclog.Logger, svc *dynamodb.Client) (*DynamoDBPlugin, error) {
	spanTable := "jaeger.spans"
	traceIDKey := "TraceID"
	spanIDKey := "SpanID"

	ctx := context.Background()

	_, err := svc.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &spanTable,
	})
	if err == nil {
		wDelete := dynamodb.NewTableNotExistsWaiter(svc)
		if err := wDelete.Wait(ctx, &dynamodb.DescribeTableInput{TableName: &spanTable}, time.Minute*5); err != nil {
			return nil, fmt.Errorf("failed waiting for table deletion, %v", err)
		}
	} else {
		var rnfe *types.ResourceNotFoundException
		if !errors.As(err, &rnfe) {
			return nil, fmt.Errorf("failed to delete table, %v", err)
		}
	}

	_, err = svc.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &traceIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: &spanIDKey, AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &spanTable,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &traceIDKey, KeyType: types.KeyTypeHash},
			{AttributeName: &spanIDKey, KeyType: types.KeyTypeRange},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table, %v", err)
	}

	wCreate := dynamodb.NewTableExistsWaiter(svc)
	if err := wCreate.Wait(ctx, &dynamodb.DescribeTableInput{TableName: &spanTable}, time.Minute*5); err != nil {
		return nil, fmt.Errorf("failed waiting for table creation, %v", err)
	}

	return &DynamoDBPlugin{
		spanWriter:        dynamospanstore.NewWriter(logger, svc, spanTable),
		spanReader:        dynamospanstore.NewReader(logger, svc, spanTable),
		archiveSpanWriter: dynamospanstore.NewWriter(logger, svc, spanTable),
		archiveSpanReader: dynamospanstore.NewReader(logger, svc, spanTable),
		dependencyReader:  dynamodependencystore.NewReader(logger, svc),

		logger: logger,
		svc:    svc,
	}, nil
}

type DynamoDBPlugin struct {
	spanWriter        *dynamospanstore.Writer
	spanReader        *dynamospanstore.Reader
	archiveSpanWriter *dynamospanstore.Writer
	archiveSpanReader *dynamospanstore.Reader
	dependencyReader  *dynamodependencystore.Reader

	logger hclog.Logger
	svc    *dynamodb.Client
}

func (h *DynamoDBPlugin) SpanWriter() spanstore.Writer {
	return h.spanWriter
}

func (h *DynamoDBPlugin) SpanReader() spanstore.Reader {
	return h.spanReader
}

func (h *DynamoDBPlugin) ArchiveSpanWriter() spanstore.Writer {
	return h.archiveSpanWriter
}

func (h *DynamoDBPlugin) ArchiveSpanReader() spanstore.Reader {
	return h.archiveSpanReader
}

func (h *DynamoDBPlugin) DependencyReader() dependencystore.Reader {
	return h.dependencyReader
}
