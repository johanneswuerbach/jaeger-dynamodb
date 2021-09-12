package plugin

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamodependencystore"
	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamospanstore"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func NewDynamoDBPlugin(logger hclog.Logger, svc *dynamodb.Client, spansTable, servicesTable, operationsTable string) (*DynamoDBPlugin, error) {
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
