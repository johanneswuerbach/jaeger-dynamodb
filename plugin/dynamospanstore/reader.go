package dynamospanstore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
)

func NewReader(logger hclog.Logger, svc *dynamodb.Client, spansTable, servicesTable, operationsTable string) *Reader {
	return &Reader{
		svc:             svc,
		spansTable:      spansTable,
		servicesTable:   servicesTable,
		operationsTable: operationsTable,
		logger:          logger,
	}
}

type Reader struct {
	logger          hclog.Logger
	svc             *dynamodb.Client
	spansTable      string
	servicesTable   string
	operationsTable string
}

func NewSpanFromSpanItem(spanItem *SpanItem) (*model.Span, error) {
	traceID, err := model.TraceIDFromString(spanItem.TraceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace id from string, %v", err)
	}

	spanID, err := model.SpanIDFromString(spanItem.SpanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get span id from string, %v", err)
	}

	return &model.Span{
		TraceID:       traceID,
		SpanID:        spanID,
		OperationName: spanItem.OperationName,
		// TODO
		References: []model.SpanRef{},
		Flags:      spanItem.Flags,
		StartTime:  time.Unix(0, spanItem.StartTime),
		Duration:   time.Duration(spanItem.Duration),
		// TODO
		Tags: []model.KeyValue{},
		// TODO
		Logs:      []model.Log{},
		Process:   NewProcessFromSpanItemProcess(spanItem.Process),
		ProcessID: spanItem.ProcessID,
		Warnings:  spanItem.Warnings,
	}, nil
}

func NewProcessFromSpanItemProcess(spanItemProcess *SpanItemProcess) *model.Process {
	return &model.Process{
		ServiceName: spanItemProcess.ServiceName,
		// TODO
		Tags: []model.KeyValue{},
	}
}

func NewServiceFromServiceItem(serviceItem *ServiceItem) string {
	return serviceItem.Name
}

func NewOperationFromOperationItem(operationItem *OperationItem) spanstore.Operation {
	return spanstore.Operation{
		Name:     operationItem.Name,
		SpanKind: "",
	}
}

func (s *Reader) getTraceByID(ctx context.Context, traceID string) (*model.Trace, error) {
	keyCond := expression.Key("TraceID").Equal(expression.Value(traceID))
	builder := expression.NewBuilder().WithKeyCondition(keyCond)
	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression, %v", err)
	}

	paginator := dynamodb.NewQueryPaginator(s.svc, &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 &s.spansTable,
	})

	spans := []*model.Span{}
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to query page: %w", err)
		}

		for _, item := range output.Items {
			spanItem := &SpanItem{}

			if err := attributevalue.UnmarshalMap(item, spanItem); err != nil {
				return nil, fmt.Errorf("failed to marshal span: %w", err)
			}

			span, err := NewSpanFromSpanItem(spanItem)
			if err != nil {
				return nil, fmt.Errorf("failed to convert span: %w", err)
			}

			spans = append(spans, span)
		}
	}

	return &model.Trace{
		Spans: spans,
	}, nil
}

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Debug("GetTrace")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer otSpan.Finish()

	return s.getTraceByID(ctx, traceID.String())
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetServices(ctx context.Context) ([]string, error) {
	s.logger.Debug("GetServices")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer otSpan.Finish()

	paginator := dynamodb.NewScanPaginator(s.svc, &dynamodb.ScanInput{
		TableName: &s.servicesTable,
	})

	services := []string{}
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to scan page: %w", err)
		}

		for _, item := range output.Items {
			serviceItem := &ServiceItem{}

			if err := attributevalue.UnmarshalMap(item, serviceItem); err != nil {
				return nil, fmt.Errorf("failed to marshal span: %w", err)
			}

			services = append(services, NewServiceFromServiceItem(serviceItem))
		}
	}

	return services, nil
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	s.logger.Debug("GetOperations", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	if query.ServiceName == "" {
		return nil, fmt.Errorf("querying without service name is not supported yet")
	}

	keyCond := expression.Key("ServiceName").Equal(expression.Value(query.ServiceName))

	// TODO support
	// SpanKind    string

	builder := expression.NewBuilder().WithKeyCondition(keyCond)
	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression, %v", err)
	}

	paginator := dynamodb.NewQueryPaginator(s.svc, &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 &s.operationsTable,
	})

	operations := []spanstore.Operation{}
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to query page, %v", err)
		}

		for _, item := range output.Items {
			operationItem := &OperationItem{}

			if err := attributevalue.UnmarshalMap(item, operationItem); err != nil {
				return nil, fmt.Errorf("failed to marshal operation, %v", err)
			}

			operations = append(operations, NewOperationFromOperationItem(operationItem))
		}
	}

	return operations, nil
}

type TraceIDResult struct {
	TraceID string
}

func (s *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	s.logger.Debug("FindTraces", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	builder := expression.NewBuilder()

	if query.ServiceName == "" {
		return nil, fmt.Errorf("querying without service name is not supported yet")
	}

	builder = builder.WithKeyCondition(expression.KeyEqual(
		expression.Key("ServiceName"), expression.Value(query.ServiceName)).And(expression.KeyBetween(
		expression.Key("StartTime"),
		expression.Value(query.StartTimeMin.UnixNano()),
		expression.Value(query.StartTimeMax.UnixNano()))))

	expressions := []expression.ConditionBuilder{}

	if query.OperationName != "" {
		expressions = append(expressions, expression.Name("OperationName").Equal(expression.Value(query.OperationName)))
	}

	if query.DurationMin != 0 {
		expressions = append(expressions, expression.Name("Duration").GreaterThanEqual(expression.Value(query.DurationMin.Nanoseconds())))
	}

	if query.DurationMax != 0 {
		expressions = append(expressions, expression.Name("Duration").LessThanEqual(expression.Value(query.DurationMax.Nanoseconds())))
	}

	if len(expressions) > 0 {
		if len(expressions) == 1 {
			builder = builder.WithFilter(expressions[0])
		} else {
			builder = builder.WithFilter(expression.And(expressions[0], expressions[1], expressions[2:]...))
		}
	}

	builder = builder.WithProjection(expression.NamesList(expression.Name("TraceID")))

	// TODO support
	// Tags          map[string]string
	// NumTraces     int

	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression, %v", err)
	}

	// Pagination, but skip after NumTraces
	output, err := s.svc.Query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 &s.spansTable,
		IndexName:                 aws.String("ServiceNameIndex"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query, %v", err)
	}

	// TODO Make this more efficient
	var traceIDResults []TraceIDResult
	if err := attributevalue.UnmarshalListOfMaps(output.Items, &traceIDResults); err != nil {
		return nil, fmt.Errorf("failed to unmarshal items, %v", err)
	}
	traceIDMap := make(map[string]struct{})
	for _, item := range traceIDResults {
		traceIDMap[item.TraceID] = struct{}{}
	}
	traceIDs := []string{}
	for k, _ := range traceIDMap {
		traceIDs = append(traceIDs, k)
	}

	// TODO Fetch trace spans in parallel
	traces := []*model.Trace{}
	for _, traceID := range traceIDs {
		trace, err := s.getTraceByID(ctx, traceID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch trace %s, %v", traceID, err)
		}
		traces = append(traces, trace)
	}

	return traces, nil
}

// This method is not used
func (s *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	s.logger.Debug("FindTraceIDs", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	return []model.TraceID{}, nil
}
