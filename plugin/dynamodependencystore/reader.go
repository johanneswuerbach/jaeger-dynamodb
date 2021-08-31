package dynamodependencystore

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/opentracing/opentracing-go"
)

func NewReader(logger hclog.Logger, svc *dynamodb.Client) *Reader {
	return &Reader{svc: svc, logger: logger}
}

type Reader struct {
	logger hclog.Logger
	svc    *dynamodb.Client
}

type Dependency struct {
	Service  string `json:"service"`
	SpanID   string `json:"span_id"`
	ParentID string `json:"parent_id"`
}

func (r *Reader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.logger.Debug("GetDependencies")
	span, _ := opentracing.StartSpanFromContext(ctx, "GetDependencies")
	defer span.Finish()

	return []model.DependencyLink{}, nil
}
