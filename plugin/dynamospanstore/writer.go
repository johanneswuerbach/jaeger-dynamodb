package dynamospanstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
)

func NewWriter(logger hclog.Logger, svc *dynamodb.Client, table string) *Writer {
	return &Writer{svc: svc, table: table, logger: logger}
}

type Writer struct {
	logger hclog.Logger
	svc    *dynamodb.Client
	table  string
}

type SpanItemProcess struct {
	ServiceName string
	// Tags          []model.KeyValue
}

type SpanItem struct {
	TraceID       string
	SpanID        string
	OperationName string
	References    []string
	Flags         model.Flags
	StartTime     int64
	Duration      int64
	// Tags          []model.KeyValue
	// Logs          []Log
	Process   *SpanItemProcess
	ProcessID string
	Warnings  []string
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
		Flags:     span.Flags,
		StartTime: span.StartTime.UnixNano(),
		Duration:  span.Duration.Nanoseconds(),
		// Tags:          span.Tags,
		// Logs:          span.Logs,
		Process:   NewSpanItemProcessFromProcess(span.Process),
		ProcessID: span.ProcessID,
		Warnings:  span.Warnings,
	}
}

func NewSpanItemProcessFromProcess(process *model.Process) *SpanItemProcess {
	return &SpanItemProcess{
		ServiceName: process.ServiceName,
		// Tags:          process.Tags,
	}
}

func (s *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	s.logger.Debug("WriteSpan")

	av, err := attributevalue.MarshalMap(NewSpanItemFromSpan(span))
	if err != nil {
		return fmt.Errorf("failed to marshal span: %w", err)
	}

	_, err = s.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}
