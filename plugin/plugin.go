package plugin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamodependencystore"
	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamospanstore"
	"golang.org/x/sync/errgroup"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	spansTable      = "jaeger.spans"
	servicesTable   = "jaeger.services"
	operationsTable = "jaeger.operations"

	traceIDKey = "TraceID"
	spanIDKey  = "SpanID"

	serviceIDKey = "Name"

	operationIDKey    = "ServiceName"
	operationRangeKey = "Name"
)

func recreateTable(ctx context.Context, svc *dynamodb.Client, input *dynamodb.CreateTableInput) error {
	_, err := svc.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: input.TableName,
	})
	if err == nil {
		wDelete := dynamodb.NewTableNotExistsWaiter(svc)
		if err := wDelete.Wait(ctx, &dynamodb.DescribeTableInput{TableName: input.TableName}, time.Minute*5); err != nil {
			return fmt.Errorf("failed waiting for table deletion, %v", err)
		}
	} else {
		var rnfe *types.ResourceNotFoundException
		if !errors.As(err, &rnfe) {
			return fmt.Errorf("failed to delete table, %v", err)
		}
	}

	_, err = svc.CreateTable(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create table, %v", err)
	}

	wCreate := dynamodb.NewTableExistsWaiter(svc)
	if err := wCreate.Wait(ctx, &dynamodb.DescribeTableInput{TableName: input.TableName}, time.Minute*5); err != nil {
		return fmt.Errorf("failed waiting for table creation, %v", err)
	}

	return nil
}

func ensureSpansTable(ctx context.Context, svc *dynamodb.Client) error {
	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &traceIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: &spanIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ServiceName"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("StartTime"), AttributeType: types.ScalarAttributeTypeN},
			// {AttributeName: aws.String("OperationName"), AttributeType: types.ScalarAttributeTypeS},
			// {AttributeName: aws.String("Duration"), AttributeType: types.ScalarAttributeTypeN},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &spansTable,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &traceIDKey, KeyType: types.KeyTypeHash},
			{AttributeName: &spanIDKey, KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("ServiceNameIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("ServiceName"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("StartTime"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType:   types.ProjectionTypeInclude,
					NonKeyAttributes: []string{"OperationName", "Duration"},
				},
			},
		},
	})
}

func ensureServicesTable(ctx context.Context, svc *dynamodb.Client) error {
	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &serviceIDKey, AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &servicesTable,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &serviceIDKey, KeyType: types.KeyTypeHash},
		},
	})
}

func ensureOperationsTable(ctx context.Context, svc *dynamodb.Client) error {
	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &operationIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: &operationRangeKey, AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &operationsTable,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &operationIDKey, KeyType: types.KeyTypeHash},
			{AttributeName: &operationRangeKey, KeyType: types.KeyTypeRange},
		},
	})
}

func NewDynamoDBPlugin(logger hclog.Logger, svc *dynamodb.Client) (*DynamoDBPlugin, error) {

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		if err := ensureSpansTable(ctx, svc); err != nil {
			return fmt.Errorf("failed to ensure spans table, %v", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := ensureServicesTable(ctx, svc); err != nil {
			return fmt.Errorf("failed to ensure services table, %v", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := ensureOperationsTable(ctx, svc); err != nil {
			return fmt.Errorf("failed to ensure operations table, %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &DynamoDBPlugin{
		spanWriter:        dynamospanstore.NewWriter(logger, svc, spansTable, servicesTable, operationsTable),
		spanReader:        dynamospanstore.NewReader(logger, svc, spansTable, servicesTable, operationsTable),
		archiveSpanWriter: dynamospanstore.NewWriter(logger, svc, spansTable, servicesTable, operationsTable),
		archiveSpanReader: dynamospanstore.NewReader(logger, svc, spansTable, servicesTable, operationsTable),
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
