package dynamodependencystore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-dynamodb/setup"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

const (
	loggerName        = "jaeger-dynamodb"
	dependenciesTable = "jaeger.dependencies"
)

func createDynamoDBSvc(assert *assert.Assertions, ctx context.Context) *dynamodb.Client {
	dynamodbURL := os.Getenv("DYNAMODB_URL")
	if dynamodbURL == "" {
		dynamodbURL = "http://localhost:8000"
	}

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		lo.Credentials = credentials.NewStaticCredentialsProvider("TEST_ONLY", "TEST_ONLY", "TEST_ONLY")
		lo.Region = "us-east-1"
		lo.EndpointResolver = aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: dynamodbURL, Source: aws.EndpointSourceCustom}, nil
			})
		return nil
	})
	assert.NoError(err)

	svc := dynamodb.NewFromConfig(cfg)

	assert.NoError(setup.PollUntilReady(ctx, svc))
	assert.NoError(setup.RecreateDependencyStoreTables(ctx, svc, &setup.SetupDependencyOptions{
		DependenciesTable: dependenciesTable,
	}))

	return svc
}

func TestGetDependencies(t *testing.T) {
	assert := assert.New(t)

	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Warn.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	ctx := context.TODO()

	svc := createDynamoDBSvc(assert, ctx)
	reader := NewReader(logger, svc, dependenciesTable)

	g, writeCtx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return WriteDependencyItem(writeCtx, svc, dependenciesTable, &DependencyItem{
			Key:            "jaeger/dynamodb-plugin",
			Parent:         "jaeger",
			Child:          "dynamodb-plugin",
			CallCount:      5,
			CallTimeBucket: TimeToBucket(time.Now()),
		})
	})
	g.Go(func() error {
		return WriteDependencyItem(writeCtx, svc, dependenciesTable, &DependencyItem{
			Key:            "jaeger/dynamodb-plugin2",
			Parent:         "jaeger",
			Child:          "dynamodb-plugin2",
			CallCount:      2,
			CallTimeBucket: TimeToBucket(time.Now()),
		})
	})
	g.Go(func() error {
		return WriteDependencyItem(writeCtx, svc, dependenciesTable, &DependencyItem{
			Key:            "jaeger/dynamodb-plugin",
			Parent:         "jaeger",
			Child:          "dynamodb-plugin",
			CallCount:      3,
			CallTimeBucket: TimeToBucket(time.Now().Add(-1 * time.Hour)),
		})
	})
	g.Go(func() error {
		return WriteDependencyItem(writeCtx, svc, dependenciesTable, &DependencyItem{
			Key:            "jaeger/dynamodb-plugin",
			Parent:         "jaeger",
			Child:          "dynamodb-plugin",
			CallCount:      15,
			CallTimeBucket: TimeToBucket(time.Now().AddDate(0, -1, 0)),
		})
	})
	assert.NoError(g.Wait())

	dependencyLinks, err := reader.GetDependencies(ctx, time.Now(), time.Hour*2)
	assert.NoError(err)
	assert.ElementsMatch(dependencyLinks, []model.DependencyLink{{
		Parent:    "jaeger",
		Child:     "dynamodb-plugin",
		CallCount: 8,
	}, {
		Parent:    "jaeger",
		Child:     "dynamodb-plugin2",
		CallCount: 2,
	}})
}
