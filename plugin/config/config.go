package config

type DynamoDBConfiguration struct {
	Endpoint string
}

type Configuration struct {
	DynamoDB DynamoDBConfiguration
}
