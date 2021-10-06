# jaeger-dynamodb

jaeger-dynamodb is gRPC storage plugin for [Jaeger](https://github.com/jaegertracing/jaeger), which uses [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) to store spans.

## Usage

* Install the plugin in your jaeger image
* Configure your AWS credentials using by providing any of the environment variable [supported by the AWS SDK for Go v2](https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/)
* Run `go main.go --create-tables=1 --only-create-tables=true` to create the necessary DynamoDB tables
