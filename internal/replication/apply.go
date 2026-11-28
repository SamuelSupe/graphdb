package replication

import (
	"context"
	"encoding/json"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
)

type ApplyEntry struct {
	Index uint64
	Data  []byte
}

// RejectedCommand represents a deterministic, non-fatal rejection (e.g. stale
// failover proposal, 409 conflict, 412 precondition failed) that must be
// recorded in the replication checkpoint without failing the Raft node.
type RejectedCommand struct {
	Response []byte
	Code     string
	Reason   string
}

// ApplyResult is the strongly typed outcome of applying a single committed
// Raft entry. Exactly one of accepted Data or non-fatal Rejected applies.
type ApplyResult struct {
	Data     []byte
	Rejected *RejectedCommand
}

// ResponseBytes returns the deterministic wire response to deliver to the
// proposing caller.
func (r ApplyResult) ResponseBytes() []byte {
	if r.Rejected != nil && len(r.Rejected.Response) > 0 {
		return r.Rejected.Response
	}
	return r.Data
}

// AcceptResult constructs a successful ApplyResult.
func AcceptResult(data []byte) ApplyResult {
	return ApplyResult{Data: data}
}

// RejectResult constructs a deterministic non-fatal rejected ApplyResult.
func RejectResult(response []byte, code, reason string) ApplyResult {
	return ApplyResult{
		Rejected: &RejectedCommand{
			Response: response,
			Code:     code,
			Reason:   reason,
		},
	}
}

// FatalMachineError marks an unrecoverable local state-machine or storage
// corruption failure that MUST halt this replica via Node.fail instead of
// advancing the applied checkpoint.
type FatalMachineError struct {
	Err error
}

func (e *FatalMachineError) Error() string {
	if e == nil || e.Err == nil {
		return "fatal state machine error"
	}
	return e.Err.Error()
}

func (e *FatalMachineError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// FatalApplyError wraps a non-nil fatal state-machine error.
func FatalApplyError(err error) *FatalMachineError {
	if err == nil {
		return nil
	}
	if existing, ok := err.(*FatalMachineError); ok {
		return existing
	}
	return &FatalMachineError{Err: err}
}

// TypedBatchStateMachine is the preferred strongly typed Raft state-machine
// contract. Deterministic business/failover conflicts are returned as
// ApplyResult{Rejected: ...} and NEVER trigger Node.fail; only a non-nil
// *FatalMachineError halts the replica.
type TypedBatchStateMachine interface {
	ApplyTypedBatch(context.Context, []ApplyEntry) ([]ApplyResult, *FatalMachineError)
}

// ApplyBatch durably applies a nonempty prefix and returns one result per
// consumed entry. Returning an error must not acknowledge any of that prefix.
// The caller never passes more than eight entries from its bounded log window.
type BatchStateMachine interface {
	ApplyBatch(context.Context, []ApplyEntry) ([][]byte, error)
}

type applyProtocolKey struct{}

// ApplyProtocol is the lowest proposal protocol in the current application.
func ApplyProtocol(ctx context.Context) int {
	protocol, _ := ctx.Value(applyProtocolKey{}).(int)
	return protocol
}

func (n *Node) applyEntries(entries []raftpb.Entry) (err error) {
	finish := n.metrics.Start("apply")
	defer func() { finish(err) }()
	typedBatcher, isTyped := n.machine.(TypedBatchStateMachine)
	batcher, canBatch := n.machine.(BatchStateMachine)
	for offset := 0; offset < len(entries); {
		var commands []proposal
		var work []ApplyEntry
		limit := offset + 1
		if (isTyped || canBatch) && entries[offset].Type == raftpb.EntryNormal && len(entries[offset].Data) > 0 {
			limit = min(offset+8, len(entries))
		}
		for _, entry := range entries[offset:limit] {
			if len(work) > 0 && (entry.Type != raftpb.EntryNormal || len(entry.Data) == 0) {
				break
			}
			var command proposal
			if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
				if err := json.Unmarshal(entry.Data, &command); err != nil {
					if len(work) > 0 {
						break
					}
					return err
				}
				if command.Protocol < 0 || command.Protocol > MaxProtocolVersion {
					return fmt.Errorf("unsupported Raft command protocol %d", command.Protocol)
				}
				if len(command.JSON) != 0 {
					if command.Protocol < JSONPayloadProtocol || len(command.Data) != 0 {
						return fmt.Errorf("invalid Raft JSON proposal envelope")
					}
					command.Data = command.JSON
				}
				if len(work) > 0 && command.Protocol != commands[0].Protocol {
					break
				}
			}
			commands = append(commands, command)
			work = append(work, ApplyEntry{Index: entry.Index, Data: command.Data})
		}
		protocol := commands[0].Protocol
		applyCtx := context.WithValue(n.ctx, applyProtocolKey{}, protocol)
		var responses [][]byte
		var applyErr error
		if isTyped {
			typedResults, fatalErr := typedBatcher.ApplyTypedBatch(applyCtx, work)
			if fatalErr != nil {
				applyErr = fatalErr
			} else {
				responses = make([][]byte, len(typedResults))
				for i, res := range typedResults {
					responses[i] = res.ResponseBytes()
				}
			}
		} else if canBatch && len(work) > 1 {
			responses, applyErr = batcher.ApplyBatch(applyCtx, work)
		} else {
			var data []byte
			data, applyErr = n.machine.Apply(applyCtx, work[0].Index, work[0].Data)
			responses = [][]byte{data}
		}
		if applyErr != nil {
			return fmt.Errorf("apply Raft entry %d: %w", work[0].Index, applyErr)
		}
		if len(responses) == 0 || len(responses) > len(work) {
			return fmt.Errorf("application consumed invalid Raft entry count %d", len(responses))
		}
		n.applicationCommits.Add(1)
		n.applicationEntries.Add(uint64(len(responses)))
		for i, data := range responses {
			n.applicationBytes.Add(-int64(len(entries[offset+i].Data)))
			n.mu.Lock()
			ch := n.proposals[commands[i].ID]
			n.mu.Unlock()
			if ch != nil {
				ch <- result{data: data}
			}
		}
		n.progress(work[len(responses)-1].Index)
		offset += len(responses)
	}
	return nil
}
