package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/johanneswuerbach/jaeger-dynamodb/plugin"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	loggerName = "jaeger-dynamodb"
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
	flag.StringVar(&configPath, "config", "", "A path to the Humio plugin's configuration file")
	flag.Parse()

	logger.Debug("Plugin starting ...")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := dynamodb.NewFromConfig(cfg)

	logger.Debug("Plugin configured")

	dynamodbPlugin := plugin.NewDynamoDBPlugin(logger, svc)
	grpc.Serve(&shared.PluginServices{
		Store:        dynamodbPlugin,
		ArchiveStore: dynamodbPlugin,
	})
}
