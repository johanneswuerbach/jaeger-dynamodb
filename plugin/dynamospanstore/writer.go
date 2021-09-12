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
	Tags        map[string]string
}

type SpanItemLog struct {
	Fields    map[string]string
	Timestamp int64
}

type SpanItem struct {
	TraceID       string
	SpanID        string
	OperationName string
	References    []string
	Flags         model.Flags
	StartTime     int64
	Duration      int64
	Tags          map[string]string
	Logs          []*SpanItemLog
	Process       *SpanItemProcess
	ServiceName   string
	ProcessID     string
	Warnings      []string
	// XXX_NoUnkeyedLiteral struct{}
	// XXX_unrecognized     []byte
	// XXX_sizecache        int32
}

func NewSpanItemFromSpan(span *model.Span) *SpanItem {
	return &SpanItem{
		TraceID:       span.TraceID.String(),
		SpanID:        span.SpanID.String(),
		OperationName: span.OperationName,
		// References:    span.References,
		Flags:       span.Flags,
		StartTime:   span.StartTime.UnixNano(),
		Duration:    span.Duration.Nanoseconds(),
		Tags:        kvToMap(span.Tags),
		Logs:        NewSpanItemLogsFromLogs(span.Logs),
		Process:     NewSpanItemProcessFromProcess(span.Process),
		ServiceName: span.Process.ServiceName,
		ProcessID:   span.ProcessID,
		Warnings:    span.Warnings,
	}
}

func NewSpanItemProcessFromProcess(process *model.Process) *SpanItemProcess {
	tags := kvToMap(process.Tags)

	return &SpanItemProcess{
		ServiceName: process.ServiceName,
		Tags:        tags,
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
	fields := kvToMap(log.Fields)

	return &SpanItemLog{
		Timestamp: log.Timestamp.UnixNano(),
		Fields:    fields,
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
