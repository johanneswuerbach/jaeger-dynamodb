package dynamodependencystore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/opentracing/opentracing-go"
)

func NewReader(logger hclog.Logger, svc *dynamodb.Client, dependenciesTable string) *Reader {
	return &Reader{
		svc:               svc,
		dependenciesTable: dependenciesTable,
		logger:            logger,
	}
}

type Reader struct {
	logger            hclog.Logger
	svc               *dynamodb.Client
	dependenciesTable string
}

func (r *Reader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.logger.Debug("GetDependencies")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetDependencies")
	defer otSpan.Finish()

	builder := expression.NewBuilder().WithFilter(
		expression.Name("CallTimeBucket").Between(
			expression.Value(TimeToBucket(endTs.Add(-lookback))), expression.Value(TimeToBucket(endTs))))

	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression, %v", err)
	}

	paginator := dynamodb.NewScanPaginator(r.svc, &dynamodb.ScanInput{
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		TableName:                 &r.dependenciesTable,
	})

	dependencyCallCounts := NewDependencyCallCounts()
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to scan page: %w", err)
		}

		for _, item := range output.Items {
			dependencyItem := &DependencyItem{}

			if err := attributevalue.UnmarshalMap(item, dependencyItem); err != nil {
				return nil, fmt.Errorf("failed to marshal span: %w", err)
			}

			dependencyCallCounts.CountRequest(dependencyItem.Parent, dependencyItem.Child, dependencyItem.CallCount)
		}
	}

	dependencyLinks := []model.DependencyLink{}
	for parent, children := range dependencyCallCounts.CallCounts {
		for child, callCount := range children {
			dependencyLinks = append(dependencyLinks, model.DependencyLink{
				Parent:    parent,
				Child:     child,
				CallCount: callCount,
			})
		}
	}

	return dependencyLinks, nil
}
