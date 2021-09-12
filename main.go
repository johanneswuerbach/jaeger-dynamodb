package main

import (
	"context"
	"log"
	"os"

	"github.com/johanneswuerbach/jaeger-dynamodb/plugin"
	pConfig "github.com/johanneswuerbach/jaeger-dynamodb/plugin/config"
	"github.com/johanneswuerbach/jaeger-dynamodb/setup"
	"github.com/ory/viper"
	"github.com/spf13/pflag"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	loggerName = "jaeger-dynamodb"

	spansTable      = "jaeger.spans"
	servicesTable   = "jaeger.services"
	operationsTable = "jaeger.operations"
)

func main() {
	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Warn.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	var configPath string
	pflag.StringVar(&configPath, "config", "", "A path to the dynamodb plugin's configuration file")
	pflag.Bool("create-tables", false, "(Re)create dynamodb table")
	pflag.Bool("only-create-tables", false, "Exit after creating dynamodb tables")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	if configPath != "" {
		viper.SetConfigFile(configPath)

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("error reading config file, %v", err)
		}
	}

	var configuration pConfig.Configuration
	err := viper.Unmarshal(&configuration)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	logger.Debug("plugin starting ...", configuration)

	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		if configuration.DynamoDB.Endpoint != "" {
			lo.Credentials = credentials.NewStaticCredentialsProvider("TEST_ONLY", "TEST_ONLY", "TEST_ONLY")
			lo.Region = "us-east-1"
			lo.EndpointResolver = aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					return aws.Endpoint{URL: configuration.DynamoDB.Endpoint, Source: aws.EndpointSourceCustom}, nil
				})
		}
		return nil
	})
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := dynamodb.NewFromConfig(cfg)

	logger.Debug("plugin configured")

	if viper.GetBool("create-tables") {
		logger.Debug("Creating tables.")
		if err := setup.RecreateTables(ctx, svc, &setup.SetupOptions{
			SpansTable:      spansTable,
			ServicesTable:   servicesTable,
			OperationsTable: operationsTable,
		}); err != nil {
			log.Fatalf("unable to create tables, %v", err)
		}
	}

	if viper.GetBool("only-create-tables") {
		logger.Debug("Exiting after tables created.")
		return
	}

	dynamodbPlugin, err := plugin.NewDynamoDBPlugin(logger, svc, spansTable, servicesTable, operationsTable)
	if err != nil {
		log.Fatalf("unable to create plugin, %v", err)
	}

	logger.Debug("plugin created")
	grpc.Serve(&shared.PluginServices{
		Store:        dynamodbPlugin,
		ArchiveStore: dynamodbPlugin,
	})
}
