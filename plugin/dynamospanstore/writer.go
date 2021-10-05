package dynamospanstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"golang.org/x/sync/errgroup"
)

func NewWriter(logger hclog.Logger, svc *dynamodb.Client, spansTable, servicesTable, operationsTable string) *Writer {
	return &Writer{
		svc:             svc,
		spansTable:      spansTable,
		servicesTable:   servicesTable,
		operationsTable: operationsTable,
		logger:          logger,
	}
}

type Writer struct {
	logger          hclog.Logger
	svc             *dynamodb.Client
	spansTable      string
	servicesTable   string
	operationsTable string
}

type SpanItemProcess struct {
	ServiceName string
	Tags        []model.KeyValue
}

type SpanItemLog struct {
	Fields    []model.KeyValue
	Timestamp int64
}

type SpanItemReference struct {
	TraceID string
	SpanID  string
	RefType model.SpanRefType
}

type SpanItem struct {
	TraceID        string
	SpanID         string
	OperationName  string
	References     []*SpanItemReference
	Flags          model.Flags
	StartTime      int64
	Duration       int64
	Tags           []model.KeyValue
	SearchableTags map[string]string
	Logs           []*SpanItemLog
	Process        *SpanItemProcess
	ServiceName    string
	ProcessID      string
	Warnings       []string
	// XXX_NoUnkeyedLiteral struct{}
	// XXX_unrecognized     []byte
	// XXX_sizecache        int32
}

func NewSpanItemFromSpan(span *model.Span) *SpanItem {
	searchableTags := append([]model.KeyValue{}, span.Tags...)
	searchableTags = append(searchableTags, span.Process.Tags...)
	for _, log := range span.Logs {
		searchableTags = append(searchableTags, log.Fields...)
	}

	return &SpanItem{
		TraceID:        span.TraceID.String(),
		SpanID:         span.SpanID.String(),
		OperationName:  span.OperationName,
		References:     NewSpanItemReferencesFromReferences(span.References),
		Flags:          span.Flags,
		StartTime:      span.StartTime.UnixNano(),
		Duration:       span.Duration.Nanoseconds(),
		Tags:           span.Tags,
		SearchableTags: kvToMap(searchableTags),
		Logs:           NewSpanItemLogsFromLogs(span.Logs),
		Process:        NewSpanItemProcessFromProcess(span.Process),
		ServiceName:    span.Process.ServiceName,
		ProcessID:      span.ProcessID,
		Warnings:       span.Warnings,
	}
}

func NewSpanItemProcessFromProcess(process *model.Process) *SpanItemProcess {
	return &SpanItemProcess{
		ServiceName: process.ServiceName,
		Tags:        process.Tags,
	}
}

func kvToMap(kvs []model.KeyValue) map[string]string {
	kvMap := map[string]string{}
	for _, field := range kvs {
		kvMap[field.Key] = field.AsString()
	}

	return kvMap
}

func NewSpanItemLogsFromLogs(logs []model.Log) []*SpanItemLog {
	spanItemLogs := []*SpanItemLog{}
	for _, log := range logs {
		spanItemLogs = append(spanItemLogs, NewSpanItemLogFromLog(&log))
	}

	return spanItemLogs
}

func NewSpanItemLogFromLog(log *model.Log) *SpanItemLog {
	return &SpanItemLog{
		Timestamp: log.Timestamp.UnixNano(),
		Fields:    log.Fields,
	}
}

func NewSpanItemReferencesFromReferences(references []model.SpanRef) []*SpanItemReference {
	spanItemReferences := []*SpanItemReference{}
	for _, reference := range references {
		spanItemReferences = append(spanItemReferences, NewSpanItemReferenceFromReference(reference))
	}

	return spanItemReferences
}

func NewSpanItemReferenceFromReference(reference model.SpanRef) *SpanItemReference {
	return &SpanItemReference{
		TraceID: reference.TraceID.String(),
		SpanID:  reference.SpanID.String(),
		RefType: reference.RefType,
	}
}

type ServiceItem struct {
	Name string
}

func NewServiceItemFromSpan(span *model.Span) *ServiceItem {
	return &ServiceItem{
		Name: span.Process.ServiceName,
	}
}

type OperationItem struct {
	Name        string
	ServiceName string
	SpanKind    string
}

func NewOperationItemFromSpan(span *model.Span) *OperationItem {
	spanKind, _ := span.GetSpanKind()

	return &OperationItem{
		Name:        span.OperationName,
		ServiceName: span.Process.ServiceName,
		SpanKind:    spanKind,
	}
}

func (s *Writer) writeItem(ctx context.Context, item interface{}, table string) error {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("failed to marshal span: %w", err)
	}

	_, err = s.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

func (s *Writer) writeSpanItem(ctx context.Context, span *model.Span) error {
	return s.writeItem(ctx, NewSpanItemFromSpan(span), s.spansTable)
}

func (s *Writer) writeServiceItem(ctx context.Context, span *model.Span) error {
	if span.Process.ServiceName == "" {
		return nil
	}
	return s.writeItem(ctx, NewServiceItemFromSpan(span), s.servicesTable)
}

func (s *Writer) writeOperationItem(ctx context.Context, span *model.Span) error {
	if span.OperationName == "" {
		return nil
	}
	return s.writeItem(ctx, NewOperationItemFromSpan(span), s.operationsTable)
}

func (s *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	g, ctx := errgroup.WithContext(context.Background())
	// TODO Writes should be batched here
	// TODO Write TTL
	g.Go(func() error {
		if err := s.writeSpanItem(ctx, span); err != nil {
			return fmt.Errorf("failed to write span item, %v", err)
		}
		return nil
	})
	// TODO Writes should be deduped here
	g.Go(func() error {
		if err := s.writeServiceItem(ctx, span); err != nil {
			return fmt.Errorf("failed to write service item, %v", err)
		}
		return nil
	})
	// TODO Writes should be deduped here
	g.Go(func() error {
		if err := s.writeOperationItem(ctx, span); err != nil {
			return fmt.Errorf("failed to write operation item, %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
