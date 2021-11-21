package setup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

const timeToLiveAttributeName = "ExpireTime"

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

	_, err = svc.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: input.TableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String(timeToLiveAttributeName),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		return fmt.Errorf("failed updating table ttl, %v", err)
	}

	return nil
}

func ensureSpansTable(ctx context.Context, svc *dynamodb.Client, tableName string) error {
	var (
		traceIDKey = "TraceID"
		spanIDKey  = "SpanID"
	)
	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &traceIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: &spanIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ServiceNameBucket"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("StartTime"), AttributeType: types.ScalarAttributeTypeN},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &tableName,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &traceIDKey, KeyType: types.KeyTypeHash},
			{AttributeName: &spanIDKey, KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("SpanSearchIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("ServiceNameBucket"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("StartTime"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType:   types.ProjectionTypeInclude,
					NonKeyAttributes: []string{"OperationName", "Duration", "SearchableTags"},
				},
			},
		},
	})
}

func ensureServicesTable(ctx context.Context, svc *dynamodb.Client, tableName string) error {
	var (
		serviceIDKey = "Name"
	)
	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &serviceIDKey, AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &tableName,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &serviceIDKey, KeyType: types.KeyTypeHash},
		},
	})
}

func ensureOperationsTable(ctx context.Context, svc *dynamodb.Client, tableName string) error {
	var (
		operationIDKey    = "ServiceName"
		operationRangeKey = "Name"
	)

	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &operationIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: &operationRangeKey, AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &tableName,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &operationIDKey, KeyType: types.KeyTypeHash},
			{AttributeName: &operationRangeKey, KeyType: types.KeyTypeRange},
		},
	})
}

func ensureDependenciesTable(ctx context.Context, svc *dynamodb.Client, tableName string) error {
	var (
		operationIDKey    = "Key"
		operationRangeKey = "CallTimeBucket"
	)

	return recreateTable(ctx, svc, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: &operationIDKey, AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: &operationRangeKey, AttributeType: types.ScalarAttributeTypeN},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   &tableName,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &operationIDKey, KeyType: types.KeyTypeHash},
			{AttributeName: &operationRangeKey, KeyType: types.KeyTypeRange},
		},
	})
}

type SetupSpanOptions struct {
	SpansTable      string
	ServicesTable   string
	OperationsTable string
}

func PollUntilReady(ctx context.Context, svc *dynamodb.Client) error {
	var err error
	for i := 0; i < 30; i++ {
		_, err = svc.ListTables(ctx, &dynamodb.ListTablesInput{})
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	return err
}

func RecreateSpanStoreTables(ctx context.Context, svc *dynamodb.Client, options *SetupSpanOptions) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := ensureSpansTable(ctx, svc, options.SpansTable); err != nil {
			return fmt.Errorf("failed to ensure spans table, %v", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := ensureServicesTable(ctx, svc, options.ServicesTable); err != nil {
			return fmt.Errorf("failed to ensure services table, %v", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := ensureOperationsTable(ctx, svc, options.OperationsTable); err != nil {
			return fmt.Errorf("failed to ensure operations table, %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

type SetupDependencyOptions struct {
	DependenciesTable string
}

func RecreateDependencyStoreTables(ctx context.Context, svc *dynamodb.Client, options *SetupDependencyOptions) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := ensureDependenciesTable(ctx, svc, options.DependenciesTable); err != nil {
			return fmt.Errorf("failed to ensure dependencies table, %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
