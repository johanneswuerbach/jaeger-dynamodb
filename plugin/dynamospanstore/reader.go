package dynamospanstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/sync/errgroup"
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

	references, err := NewReferencesFromFromSpanItemReferences(spanItem.References)
	if err != nil {
		return nil, fmt.Errorf("failed to get convert references, %v", err)
	}

	return &model.Span{
		TraceID:       traceID,
		SpanID:        spanID,
		OperationName: spanItem.OperationName,
		References:    references,
		Flags:         spanItem.Flags,
		StartTime:     time.Unix(0, spanItem.StartTime),
		Duration:      time.Duration(spanItem.Duration),
		Tags:          spanItem.Tags,
		Logs:          NewLogsFromFromSpanItemLogs(spanItem.Logs),
		Process:       NewProcessFromSpanItemProcess(spanItem.Process),
		ProcessID:     spanItem.ProcessID,
		Warnings:      spanItem.Warnings,
	}, nil
}

func NewLogsFromFromSpanItemLogs(spanItemLogs []*SpanItemLog) []model.Log {
	logs := []model.Log{}
	for _, spanItemLog := range spanItemLogs {
		logs = append(logs, NewLogFromFromSpanItemLog(spanItemLog))
	}

	return logs
}

func NewLogFromFromSpanItemLog(log *SpanItemLog) model.Log {
	return model.Log{
		Timestamp: time.Unix(0, log.Timestamp),
		Fields:    log.Fields,
	}
}

func NewReferencesFromFromSpanItemReferences(spanItemReferences []*SpanItemReference) ([]model.SpanRef, error) {
	references := []model.SpanRef{}
	for _, spanItemReference := range spanItemReferences {
		reference, err := NewReferenceFromFromSpanItemReference(spanItemReference)
		if err != nil {
			return nil, err
		}
		references = append(references, *reference)
	}

	return references, nil
}

func NewReferenceFromFromSpanItemReference(spanItemReference *SpanItemReference) (*model.SpanRef, error) {
	traceID, err := model.TraceIDFromString(spanItemReference.TraceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace id from string, %v", err)
	}

	spanID, err := model.SpanIDFromString(spanItemReference.SpanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get span id from string, %v", err)
	}

	return &model.SpanRef{
		TraceID: traceID,
		SpanID:  spanID,
		RefType: spanItemReference.RefType,
	}, nil
}

func NewProcessFromSpanItemProcess(spanItemProcess *SpanItemProcess) *model.Process {
	return &model.Process{
		ServiceName: spanItemProcess.ServiceName,
		Tags:        spanItemProcess.Tags,
	}
}

func NewServiceFromServiceItem(serviceItem *ServiceItem) string {
	return serviceItem.Name
}

func NewOperationFromOperationItem(operationItem *OperationItem) spanstore.Operation {
	return spanstore.Operation{
		Name:     operationItem.Name,
		SpanKind: operationItem.SpanKind,
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

	if len(spans) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return &model.Trace{
		Spans: spans,
	}, nil
}

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Trace("GetTrace", traceID.String())
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer otSpan.Finish()

	return s.getTraceByID(ctx, traceID.String())
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetServices(ctx context.Context) ([]string, error) {
	s.logger.Trace("GetServices")
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
	sort.Strings(services)

	return services, nil
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	s.logger.Trace("GetOperations", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	if query.ServiceName == "" {
		return nil, fmt.Errorf("querying without service name is not supported yet")
	}

	keyCond := expression.Key("ServiceName").Equal(expression.Value(query.ServiceName))
	builder := expression.NewBuilder().WithKeyCondition(keyCond)

	if query.SpanKind != "" {
		builder = builder.WithFilter(expression.Name("SpanKind").Equal(expression.Value(query.SpanKind)))
	}

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

type TraceIDSet struct {
	m map[string]interface{}
	sync.RWMutex
}

func NewTraceIDSet() *TraceIDSet {
	return &TraceIDSet{
		m: map[string]interface{}{},
	}
}

func (s *TraceIDSet) Add(item string) {
	s.Lock()
	defer s.Unlock()

	s.m[item] = struct{}{}
}

func (s *TraceIDSet) Len() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.m)
}

func (s *TraceIDSet) Items() []string {
	s.RLock()
	defer s.RUnlock()

	traceIDs := []string{}
	for k := range s.m {
		traceIDs = append(traceIDs, k)
	}

	return traceIDs
}

func (s *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	s.logger.Trace("FindTraces", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	if query.ServiceName == "" {
		return nil, fmt.Errorf("querying without service name is not supported yet")
	}

	scanGroup, scanCtx := errgroup.WithContext(ctx)
	traceIDSet := NewTraceIDSet()
	for i := 0; i < serviceNameBuckets; i++ {
		serviceNameBucket := i
		// Fanout against all span buckets to find matching spans
		scanGroup.Go(func() error {
			builder := expression.NewBuilder()
			builder = builder.WithKeyCondition(expression.KeyEqual(
				expression.Key("ServiceNameBucket"), expression.Value(toServiceNameBucket(query.ServiceName, serviceNameBucket))).And(expression.KeyBetween(
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

			for key, value := range query.Tags {
				expressions = append(expressions, expression.Name(fmt.Sprintf("SearchableTags.%s", key)).Equal(expression.Value(value)))
			}

			if len(expressions) > 0 {
				if len(expressions) == 1 {
					builder = builder.WithFilter(expressions[0])
				} else {
					builder = builder.WithFilter(expression.And(expressions[0], expressions[1], expressions[2:]...))
				}
			}

			builder = builder.WithProjection(expression.NamesList(expression.Name("TraceID")))

			expr, err := builder.Build()
			if err != nil {
				return fmt.Errorf("failed to build query expression, %v", err)
			}

			paginator := dynamodb.NewQueryPaginator(s.svc, &dynamodb.QueryInput{
				KeyConditionExpression:    expr.KeyCondition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				FilterExpression:          expr.Filter(),
				ProjectionExpression:      expr.Projection(),
				TableName:                 &s.spansTable,
				IndexName:                 aws.String("SpanSearchIndex"),
				ScanIndexForward:          aws.Bool(false),
			})

			for traceIDSet.Len() < query.NumTraces && paginator.HasMorePages() {
				output, err := paginator.NextPage(scanCtx)
				if err != nil {
					return fmt.Errorf("failed to query page, %v", err)
				}

				var traceIDResults []TraceIDResult
				if err := attributevalue.UnmarshalListOfMaps(output.Items, &traceIDResults); err != nil {
					return fmt.Errorf("failed to unmarshal items, %v", err)
				}
				for _, item := range traceIDResults {
					if traceIDSet.Len() >= query.NumTraces {
						break
					}
					traceIDSet.Add(item.TraceID)
				}
			}

			return nil
		})
	}
	if err := scanGroup.Wait(); err != nil {
		return nil, fmt.Errorf("failed to query span search index, %v", err)
	}

	traceIDs := traceIDSet.Items()
	tracesChan := make(chan *model.Trace, len(traceIDs))
	getGroup, getCtx := errgroup.WithContext(ctx)
	for _, traceID := range traceIDs {
		traceID := traceID
		// TODO Might be better to use BatchGetItem here
		getGroup.Go(func() error {
			trace, err := s.getTraceByID(getCtx, traceID)
			if err != nil {
				return fmt.Errorf("failed to fetch trace %s, %v", traceID, err)
			}
			tracesChan <- trace
			return nil
		})
	}
	if err := getGroup.Wait(); err != nil {
		return nil, fmt.Errorf("failed to fetch traces, %v", err)
	}
	close(tracesChan)

	traces := []*model.Trace{}
	for trace := range tracesChan {
		traces = append(traces, trace)
	}

	return traces, nil
}

// This method is not used
func (s *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	s.logger.Trace("FindTraceIDs", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	return []model.TraceID{}, nil
}
