package config

type DynamoDBConfiguration struct {
	Endpoint       string
	RecreateTables bool
}

type Configuration struct {
	DynamoDB DynamoDBConfiguration
}
