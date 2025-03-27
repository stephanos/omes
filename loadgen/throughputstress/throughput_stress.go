package throughputstress

import (
	"fmt"
	"strconv"
	"strings"
)

// WorkflowParams is the single input for the throughput stress workflow.
type WorkflowParams struct {
	// Number of times we should loop through the steps in the workflow.
	Iterations int `json:"iterations"`
	// If true, skip sleeps. This makes workflow end to end latency more informative.
	SkipSleep bool `json:"skipSleep"`
	// What iteration to start on. If we have continued-as-new, we might be starting at a nonzero
	// number.
	InitialIteration int `json:"initialIteration"`
	// If nonzero, we will continue as new after history has grown to be at least this many events.
	ContinueAsNewAfterEventCount int `json:"continueAsNewAfterEventCount"`

	// Set internally and incremented every time the workflow continues as new.
	TimesContinued int `json:"timesContinued"`
	// Set internally and incremented every time the workflow spawns a child.
	ChildrenSpawned int `json:"childrenSpawned"`

	// If set, the workflow will run nexus tests.
	// The endpoint should be created ahead of time.
	NexusEndpoint string `json:"nexusEndpoint"`

	// The SleepActivity sleeps for a random amount of time, weighted by the values in this map.
	// The keys are the sleep durations (in seconds), and the values are the weights. Higher weight
	// means higher probability. If there's a single value in the map, it will be used as-is.
	// If there are no values, no SleepActivity will be run.
	SleepActivityDistribution map[int]int
}

type WorkflowOutput struct {
	// The total number of children that were spawned across all continued runs of the workflow.
	ChildrenSpawned int `json:"childrenSpawned"`
	// The total number of times the workflow continued as new.
	TimesContinued int `json:"timesContinued"`
}

func ParseSleepActivityDistribution(input string) (map[int]int, error) {
	res := make(map[int]int)
	pairs := strings.Split(input, ",")
	for _, pair := range pairs {
		kv := strings.Split(strings.TrimSpace(pair), ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format, expected colon-separated number pair: %s", pair)
		}
		key, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil || key <= 0 {
			return nil, fmt.Errorf("invalid sleep duration: %s", pair)
		}
		value, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid weight: %s", pair)
		}
		res[key] = value
	}
	return res, nil
}
