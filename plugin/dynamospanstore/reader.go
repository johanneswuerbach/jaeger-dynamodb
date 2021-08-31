package dynamospanstore

import (
	"context"

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

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Debug("GetTrace")
	span, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer span.Finish()

	var trace = model.Trace{
		Spans: []*model.Span{},
	}
	return &trace, nil
}

// TODO beggningOfTime might not be a good idea, maybe make a system property that the image is run with?
func (s *Reader) GetServices(ctx context.Context) ([]string, error) {
	s.logger.Debug("GetServices")
	span, _ := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer span.Finish()

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
