package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/prozz/aws-embedded-metrics-golang/emf"

	"github.com/johanneswuerbach/jaeger-dynamodb/plugin/dynamodependencystore"
)

type SpanItemReference struct {
	TraceID string
	SpanID  string
}

func (s *SpanItemReference) Key() string {
	return fmt.Sprintf("%s/%s", s.TraceID, s.SpanID)
}

// Subset of the full type
type SpanItem struct {
	TraceID     string
	SpanID      string
	References  []*SpanItemReference
	ServiceName string
}

func (s *SpanItem) Key() string {
	return fmt.Sprintf("%s/%s", s.TraceID, s.SpanID)
}

type DependencyCallCounts map[string]map[string]uint64

var svc *dynamodb.Client

const (
	tableName = "jaeger.dependencies" // TODO: Move to an environment variable
)

func init() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	svc = dynamodb.NewFromConfig(cfg)
}

type DynamoDBAPI interface {
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

func calculateDependencyCallsInBatch(ctx context.Context, e events.DynamoDBEvent, m *emf.Logger) (*dynamodependencystore.DependencyCallCounts, error) {
	idsToService := map[string]string{}
	// Build a map of all (trace id, span id) ~> service name in the batch

	totalRecords := len(e.Records)
	fmt.Println("Received records", totalRecords)
	m.Metric("totalRecords", totalRecords)

	spans := make([]*SpanItem, totalRecords)
	for i, record := range e.Records {

		element := record.Change.NewImage
		elementReferences := element["References"].List()
		references := make([]*SpanItemReference, len(elementReferences))
		for i, elementReference := range elementReferences {
			ref := elementReference.Map()
			references[i] = &SpanItemReference{
				TraceID: ref["TraceID"].String(),
				SpanID:  ref["SpanID"].String(),
			}
		}

		spanItem := &SpanItem{
			TraceID:     element["TraceID"].String(),
			SpanID:      element["SpanID"].String(),
			References:  references,
			ServiceName: element["ServiceName"].String(),
		}

		spans[i] = spanItem
		idsToService[spanItem.Key()] = spanItem.ServiceName
	}

	// Resolve all dependencies, lookup missing dependencies, ignore not found errors
	includedSpans := 0
	fetchedSpans := 0
	dependencyCallCounts := dynamodependencystore.NewDependencyCallCounts()
	for _, span := range spans {
		for _, reference := range span.References {
			val, ok := idsToService[reference.Key()]
			if ok {
				includedSpans += 1
				dependencyCallCounts.CountRequest(val, span.ServiceName, 1)
			} else {
				fetchedSpans += 1
				// TODO: Fetch span
			}
		}
	}

	m.Metric("includedSpans", includedSpans)
	m.Metric("fetchedSpans", fetchedSpans)

	return dependencyCallCounts, nil
}

func updateDependencyCalls(ctx context.Context, e events.DynamoDBEvent, m *emf.Logger, svc DynamoDBAPI) error {
	dependencyCallCounts, err := calculateDependencyCallsInBatch(ctx, e, m)
	if err != nil {
		return fmt.Errorf("failed to calculate dependency call count: %w", err)
	}

	// Write results to current hour
	for parent, children := range dependencyCallCounts.CallCounts {
		for child, callCount := range children {
			if err := dynamodependencystore.WriteDependencyItem(ctx, svc, tableName, &dynamodependencystore.DependencyItem{
				Key:            fmt.Sprintf("%s/%s", parent, child),
				Parent:         parent,
				Child:          child,
				CallCount:      callCount,
				CallTimeBucket: dynamodependencystore.TimeToBucket(time.Now()),
			}); err != nil {
				return fmt.Errorf("failed to write dependency item: %w", err)
			}
		}
	}

	return nil
}

func handleRequest(ctx context.Context, e events.DynamoDBEvent) error {
	m := emf.New()
	defer m.Log()

	if err := updateDependencyCalls(ctx, e, m, svc); err != nil {
		fmt.Printf("error occured during lambda process, %s", err)
		panic(err)
	}

	return nil
}

func main() {
	// Make the handler available for Remote Procedure Call by AWS Lambda
	lambda.Start(handleRequest)
}
