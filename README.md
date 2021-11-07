# jaeger-dynamodb

jaeger-dynamodb is gRPC storage plugin for [Jaeger](https://github.com/jaegertracing/jaeger), which uses [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) to store spans.

## Usage

### Prepare your environment

Prepare our environment using terraform a comparable tool. The table definitions are not final yet and might change in the future.

```tf
locals {
  tables = ["jaeger.spans", "jaeger.services", "jaeger.operations"]
}

data "aws_iam_policy_document" "jaeger" {
  statement {
    actions = [
      "dynamodb:Scan",
      "dynamodb:Query",
    ]

    resources = [for _, table in local.tables : "arn:aws:dynamodb:*:*:table/${table}/index/*"]
  }

  statement {
    actions = [
      "dynamodb:PutItem",
      "dynamodb:Scan",
      "dynamodb:Query",
    ]

    resources = [for _, table in local.tables : "arn:aws:dynamodb:*:*:table/${table}"]
  }
}

# This assumes you are using kube2iam https://github.com/jtblin/kube2iam#iam-roles and should
# be adjusted if you use kiam https://github.com/uswitch/kiam or EKS https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html

data "aws_iam_policy_document" "k8s_nodes_assumerole" {
  statement {
    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }

  statement {
    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::123456789012:role/kubernetes-worker-role"]
    }
  }
}

resource "aws_iam_role" "jaeger" {
  name               = "jaeger"
  assume_role_policy = data.aws_iam_policy_document.k8s_nodes_assumerole.json
}

resource "aws_iam_role_policy" "jaeger" {
  name   = "jaeger"
  role   = aws_iam_role.jaeger.id
  policy = data.aws_iam_policy_document.jaeger.json
}

resource "aws_dynamodb_table" "jaeger_spans" {
  name         = "jaeger.spans"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "TraceID"
    type = "S"
  }

  attribute {
    name = "SpanID"
    type = "S"
  }

  attribute {
    name = "ServiceName"
    type = "S"
  }

  attribute {
    name = "StartTime"
    type = "N"
  }

  ttl {
    attribute_name = "ExpireTime"
    enabled        = true
  }

  hash_key  = "TraceID"
  range_key = "SpanID"

  server_side_encryption {
    enabled = "true"
  }

  point_in_time_recovery {
    enabled = "true"
  }

  global_secondary_index {
    name               = "ServiceNameIndex"
    hash_key           = "ServiceName"
    range_key          = "StartTime"
    projection_type    = "INCLUDE"
    non_key_attributes = ["OperationName", "Duration", "SearchableTags"]
  }
}


resource "aws_dynamodb_table" "jaeger_services" {
  name         = "jaeger.services"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "Name"
    type = "S"
  }

  ttl {
    attribute_name = "ExpireTime"
    enabled        = true
  }

  hash_key = "Name"

  server_side_encryption {
    enabled = "true"
  }

  point_in_time_recovery {
    enabled = "true"
  }
}


resource "aws_dynamodb_table" "jaeger_operations" {
  name         = "jaeger.operations"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "ServiceName"
    type = "S"
  }

  attribute {
    name = "Name"
    type = "S"
  }

  ttl {
    attribute_name = "ExpireTime"
    enabled        = true
  }

  hash_key  = "ServiceName"
  range_key = "Name"

  server_side_encryption {
    enabled = "true"
  }

  point_in_time_recovery {
    enabled = "true"
  }
}

resource "aws_dynamodb_table" "jaeger_dependencies" {
  name         = "jaeger.dependencies"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "Key"
    type = "S"
  }

  attribute {
    name = "CallTimeBucket"
    type = "N"
  }

  ttl {
    attribute_name = "ExpireTime"
    enabled        = true
  }

  hash_key  = "Key"
  range_key = "CallTimeBucket"

  server_side_encryption {
    enabled = "true"
  }

  point_in_time_recovery {
    enabled = "true"
  }
}

// Lambda to compute the dependencies between services

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type = "Service"

      identifiers = [
        "lambda.amazonaws.com",
      ]
    }

    effect = "Allow"
  }
}

resource "aws_iam_role" "jaeger_dependencies_lambda" {
  name               = "jaeger_dependencies_lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

locals {
  jaeger_spans_table        = "jaeger.spans"
  jaeger_dependencies_table = "jaeger.dependencies"
}


data "aws_iam_policy_document" "jaeger_dependencies_lambda" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "arn:aws:logs:*:*:*",
    ]
  }

  statement {
    actions = [
      "dynamodb:DescribeStream",
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:ListStreams"
    ]

    resources = [
      "arn:aws:dynamodb:*:*:table/${local.jaeger_spans_table}/stream/*"
    ]
  }

  statement {
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchUpdateItem",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]

    resources = [
      "arn:aws:dynamodb:*:*:table/${local.jaeger_dependencies_table}"
    ]
  }
}

resource "aws_iam_policy" "jaeger_dependencies_lambda" {
  name   = "jaeger_dependencies_lambda"
  policy = data.aws_iam_policy_document.jaeger_dependencies_lambda.json
}

resource "aws_iam_role_policy_attachment" "jaeger_dependencies_lambda" {
  role       = aws_iam_role.jaeger_dependencies_lambda.name
  policy_arn = aws_iam_policy.jaeger_dependencies_lambda.arn
}

resource "aws_lambda_function" "jaeger_dependencies" {
  filename      = "${path.module}/dependency-lambda.zip"
  function_name = "jaeger_dependencies"
  role          = aws_iam_role.jaeger_dependencies_lambda.arn
  handler       = "bootstrap"

  source_code_hash = filebase64sha256("${path.module}/dependency-lambda.zip")

  architectures = ["arm64"]
  runtime       = "provided.al2"
  memory_size   = "512"
  timeout       = 300
}

resource "aws_lambda_event_source_mapping" "jaeger_dependencies_lambda" {
  event_source_arn                   = aws_dynamodb_table.jaeger_spans.stream_arn
  function_name                      = aws_lambda_function.jaeger_dependencies.arn
  starting_position                  = "LATEST"
  batch_size                         = 10000
  maximum_batching_window_in_seconds = 300
}
```

### Install the plugin

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: jaeger-dynamodb
  namespace: jaeger-collector
data:
  config.yaml: ""
---
apiVersion: v1
kind: Secret
metadata:
  name: jaeger
  namespace: jaeger-collector
type: Opaque
data:
  AWS_REGION: ZXUtd2VzdC0x # encode your region (us-east-1) in this case
---
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger
  namespace: jaeger-collector
spec:
  strategy: production
  collector:
    maxReplicas: 10
    options:
      collector:
        # queue size and memory requests / limits based on
        # https://github.com/jaegertracing/jaeger-operator/issues/872#issuecomment-596618094
        queue-size-memory: 64
    resources:
      requests:
        memory: 128Mi
        cpu: "150m"
      limits:
        memory: 512Mi
        cpu: "500m"
  query:
    replicas: 2
    resources:
      requests:
        memory: 125Mi
        cpu: "150m"
      limits:
        memory: 1024Mi
        cpu: "500m"
  annotations:
    iam.amazonaws.com/role: jaeger
  storage:
    type: grpc-plugin
    grpcPlugin:
      image: ghcr.io/johanneswuerbach/jaeger-dynamodb:v0.0.6
    options:
      grpc-storage-plugin:
        binary: /plugin/jaeger-dynamodb
        configuration-file: /plugin-config/config.yaml
        log-level: debug
    esIndexCleaner:
      enabled: false
    dependencies:
      enabled: false
    # Not really a secret, but there is no other way to get environment
    # variables into the container currently
    secretName: jaeger
  volumeMounts:
    - name: plugin-config
      mountPath: /plugin-config
  volumes:
    - name: plugin-config
      configMap:
        name: jaeger-dynamodb
```
