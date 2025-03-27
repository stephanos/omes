package throughputstress

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

type Activities struct {
	Client client.Client
}

type PayloadActivityInput struct {
	IgnoredInputData []byte
	// DesiredOutputSize determines the size of the output data in bytes, filled randomly.
	DesiredOutputSize int
}

type SleepActivityInput struct {
	SleepDuration time.Duration
}

func MakePayloadInput(inSize, outSize int) *PayloadActivityInput {
	inDat := make([]byte, inSize)
	rand.Read(inDat)
	return &PayloadActivityInput{
		IgnoredInputData:  inDat,
		DesiredOutputSize: outSize,
	}
}

func MakeSleepInput(distribution map[int]int) *SleepActivityInput {
	if len(distribution) == 0 {
		return nil
	}
	if len(distribution) == 1 {
		for k := range distribution {
			return &SleepActivityInput{SleepDuration: time.Duration(k) * time.Second}
		}
	}

	// Extract and sort keys (values)
	keys := make([]int, 0, len(distribution))
	for v := range distribution {
		keys = append(keys, v)
	}
	sort.Ints(keys)
	n := len(keys)

	// Compute cumulative weights for sampling
	weights := make([]int, n)
	cdf := make([]int, n)
	totalWeight := 0
	for i, k := range keys {
		weights[i] = distribution[k]
		totalWeight += weights[i]
		cdf[i] = totalWeight
	}

	// Pick a random number in the cumulative weight range
	target := rand.Intn(totalWeight)
	idx := sort.SearchInts(cdf, target)
	if idx == 0 {
		idx = 1
	}

	v1, v2 := keys[idx-1], keys[idx]
	interp := v1 + rand.Intn(v2-v1+1)
	return &SleepActivityInput{SleepDuration: time.Duration(time.Duration(interp) * time.Second)}
}

// Payload serves no purpose other than to accept inputs and return outputs of a
// specific size.
func (a *Activities) Payload(_ context.Context, in *PayloadActivityInput) ([]byte, error) {
	output := make([]byte, in.DesiredOutputSize)
	//goland:noinspection GoDeprecation -- This is fine. We don't need crypto security.
	rand.Read(output)
	return output, nil
}

// TODO
func (a *Activities) Sleep(_ context.Context, in *SleepActivityInput) error {
	fmt.Println()
	time.Sleep(in.SleepDuration)
	return nil
}

func (a *Activities) SelfQuery(ctx context.Context, queryType string) error {
	info := activity.GetInfo(ctx)
	wid := info.WorkflowExecution.ID

	resp, err := a.Client.QueryWorkflowWithOptions(
		ctx,
		&client.QueryWorkflowWithOptionsRequest{
			WorkflowID: wid,
			QueryType:  queryType,
		},
	)

	if err != nil {
		return err
	}

	if resp.QueryRejected != nil {
		return fmt.Errorf("query rejected: %s", resp.QueryRejected)
	}

	return nil
}

func (a *Activities) SelfDescribe(ctx context.Context) error {
	info := activity.GetInfo(ctx)
	wid := info.WorkflowExecution.ID

	_, err := a.Client.DescribeWorkflowExecution(ctx, wid, "")
	if err != nil {
		return err
	}
	return nil
}

func (a *Activities) SelfUpdate(ctx context.Context, updateName string) error {
	we := activity.GetInfo(ctx).WorkflowExecution
	handle, err := a.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   we.ID,
		RunID:        we.RunID,
		UpdateName:   updateName,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	return handle.Get(ctx, nil)
}

func (a *Activities) SelfSignal(ctx context.Context, signalName string) error {
	we := activity.GetInfo(ctx).WorkflowExecution
	return a.Client.SignalWorkflow(ctx, we.ID, we.RunID, signalName, nil)
}
