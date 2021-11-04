package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/prozz/aws-embedded-metrics-golang/emf"
	"github.com/stretchr/testify/assert"
)

func TestCalculateDependencyCallsInBatch(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event := &events.DynamoDBEvent{}
	fixture, err := ioutil.ReadFile("./fixtures/event.json")
	assert.NoError(err)
	assert.NoError(json.Unmarshal(fixture, event))
	m := emf.New()

	dependencyCallCounts, err := calculateDependencyCallsInBatch(ctx, *event, m)
	assert.NoError(err)
	assert.Equal(dependencyCallCounts.CallCounts, map[string]map[string]uint64{
		"thanos-query": {
			"thanos-sidecar": 1,
		},
	})
}
