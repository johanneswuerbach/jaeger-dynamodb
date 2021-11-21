module github.com/johanneswuerbach/jaeger-dynamodb/dependency-lambda

go 1.17

require (
	github.com/aws/aws-lambda-go v1.27.0
	github.com/aws/aws-sdk-go-v2/config v1.10.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.8.0
	github.com/johanneswuerbach/jaeger-dynamodb v0.0.0
	github.com/prozz/aws-embedded-metrics-golang v1.2.0
	github.com/stretchr/testify v1.7.0
)

require (
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.3.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/hashicorp/go-hclog v0.16.2 // indirect
	github.com/jaegertracing/jaeger v1.28.0 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/uber/jaeger-client-go v2.29.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	golang.org/x/sys v0.0.0-20211004093028-2c5d950f24ef // indirect
)

require (
	github.com/aws/aws-sdk-go-v2 v1.11.0 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.6.1 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.8.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.0.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.3.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.6.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.10.0 // indirect
	github.com/aws/smithy-go v1.9.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/johanneswuerbach/jaeger-dynamodb v0.0.0 => ../
