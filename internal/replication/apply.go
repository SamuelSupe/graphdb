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

// ApplyBatch durably applies a nonempty prefix and returns one result per
// consumed entry. Returning an error must not acknowledge any of that prefix.
// The caller never passes more than eight entries from its bounded log window.
type BatchStateMachine interface {
	ApplyBatch(context.Context, []ApplyEntry) ([][]byte, error)
}

func (n *Node) applyEntries(entries []raftpb.Entry) (err error) {
	finish := n.metrics.Start("apply")
	defer func() { finish(err) }()
	batcher, canBatch := n.machine.(BatchStateMachine)
	for offset := 0; offset < len(entries); {
		var commands []proposal
		var work []ApplyEntry
		limit := offset + 1
		if canBatch && entries[offset].Type == raftpb.EntryNormal && len(entries[offset].Data) > 0 {
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
			}
			commands = append(commands, command)
			work = append(work, ApplyEntry{Index: entry.Index, Data: command.Data})
		}
		var responses [][]byte
		var err error
		if canBatch && len(work) > 1 {
			responses, err = batcher.ApplyBatch(n.ctx, work)
		} else {
			var data []byte
			data, err = n.machine.Apply(n.ctx, work[0].Index, work[0].Data)
			responses = [][]byte{data}
		}
		if err != nil {
			return fmt.Errorf("apply Raft entry %d: %w", work[0].Index, err)
		}
		if len(responses) == 0 || len(responses) > len(work) {
			return fmt.Errorf("application consumed invalid Raft entry count %d", len(responses))
		}
		n.applicationCommits.Add(1)
		n.applicationEntries.Add(uint64(len(responses)))
		n.progress(work[len(responses)-1].Index)
		for i, data := range responses {
			n.applicationBytes.Add(-int64(len(entries[offset+i].Data)))
			n.mu.Lock()
			ch := n.proposals[commands[i].ID]
			n.mu.Unlock()
			if ch != nil {
				ch <- result{data: data}
			}
		}
		offset += len(responses)
	}
	return nil
}
