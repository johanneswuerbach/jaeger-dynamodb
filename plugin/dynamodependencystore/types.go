package dynamodependencystore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DependencyItem struct {
	Key            string
	Parent         string
	Child          string
	CallCount      uint64
	Source         string
	CallTimeBucket int64
	// XXX_NoUnkeyedLiteral struct{} `json:"-"`
	// XXX_unrecognized     []byte   `json:"-"`
	// XXX_sizecache        int32    `json:"-"`
}

func TimeToBucket(t time.Time) int64 {
	return t.Truncate(1*time.Hour).UnixMilli() / 1000
}

type DependencyCallCounts struct {
	CallCounts map[string]map[string]uint64
}

func NewDependencyCallCounts() *DependencyCallCounts {
	return &DependencyCallCounts{
		CallCounts: map[string]map[string]uint64{},
	}
}

func (d *DependencyCallCounts) CountRequest(parent, child string, count uint64) {
	children, ok := d.CallCounts[parent]
	if !ok {
		d.CallCounts[parent] = map[string]uint64{child: count}
		return
	}

	currentCount, ok := children[child]
	if !ok {
		children[child] = count
	} else {
		children[child] = currentCount + count
	}
}

type DynamoDBAPI interface {
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

func WriteDependencyItem(ctx context.Context, svc DynamoDBAPI, dependenciesTable string, item *DependencyItem) error {
	builder := expression.NewBuilder().WithUpdate(expression.
		Add(expression.Name("CallCount"), expression.Value(item.CallCount)).
		Set(expression.Name("Parent"), expression.Value(item.Parent)).
		Set(expression.Name("Child"), expression.Value(item.Child)).
		Set(expression.Name("Source"), expression.Value(item.Source)))
	expr, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build update expression, %v", err)
	}

	_, err = svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"Key": &types.AttributeValueMemberS{
				Value: item.Key,
			},
			"CallTimeBucket": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(item.CallTimeBucket, 10),
			},
		},
		TableName:                 aws.String(dependenciesTable),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}
	return nil
}
