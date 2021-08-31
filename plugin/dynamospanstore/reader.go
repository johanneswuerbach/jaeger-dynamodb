package dynamospanstore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
)

func NewReader(logger hclog.Logger, svc *dynamodb.Client, table string) *Reader {
	return &Reader{svc: svc, table: table, logger: logger}
}

type Reader struct {
	logger hclog.Logger
	svc    *dynamodb.Client
	table  string
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

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Debug("GetTrace")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer otSpan.Finish()

	keyCond := expression.Key("TraceID").Equal(expression.Value(traceID.String()))
	builder := expression.NewBuilder().WithKeyCondition(keyCond)
	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression, %v", err)
	}

	output, err := s.svc.Query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 &s.table,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query, %v", err)
	}

	// TODO: Pagination
	spans := make([]*model.Span, len(output.Items))

	for i, item := range output.Items {
		spanItem := &SpanItem{}

		if err := attributevalue.UnmarshalMap(item, spanItem); err != nil {
			return nil, fmt.Errorf("failed to marshal span: %w", err)
		}

		span, err := NewSpanFromSpanItem(spanItem)
		if err != nil {
			return nil, fmt.Errorf("failed to convert span: %w", err)
		}

		spans[i] = span
	}

	var trace = model.Trace{
		Spans: spans,
	}
	return &trace, nil
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetServices(ctx context.Context) ([]string, error) {
	s.logger.Debug("GetServices")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer otSpan.Finish()

	return []string{}, nil
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	s.logger.Debug("GetOperations")
	span, _ := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	return []spanstore.Operation{}, nil
}

func (s *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	s.logger.Debug("FindTraces")
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	return []*model.Trace{}, nil
}

// This method is not used
func (s *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	s.logger.Debug("FindTraceIDs")
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	return []model.TraceID{}, nil
}
