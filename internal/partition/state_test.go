package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestGraphCommitRequiresAllPreparesAndSurvivesCoordinatorRestart(t *testing.T) {
	ctx := context.Background()
	controlObjects := storage.NewMemoryStore()
	control := Store{Objects: controlObjects, Prefix: "graphdb"}
	data := []Store{{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}, {Objects: storage.NewMemoryStore(), Prefix: "graphdb"}}
	apply := func(s Store, a Action) any {
		t.Helper()
		a.Tenant = "t"
		a.Generation = 1
		v, err := s.Apply(ctx, a)
		if err != nil {
			t.Fatalf("%s: %v", a.Operation, err)
		}
		return v
	}
	g := Graph{Format: FormatVersion, Tenant: "t", Generation: 1}
	for i := range g.Partitions {
		g.Partitions[i] = Placement{Group: fmt.Sprintf("g%d", i%2), Epoch: 1}
	}
	apply(control, Action{Operation: "initialize", Graph: &g})
	input := []byte(`{"mutations":{}}`)
	digest := fmt.Sprintf("%x", sha256.Sum256(input))
	stage := "11111111111111111111111111111111"
	ref := apply(control, Action{Operation: "stage_input", Txn: stage, Data: input, Digest: digest}).(Input)
	txn := apply(control, Action{Operation: "begin", Txn: stage, Identity: "key", Digest: digest, Inputs: []Input{ref}, At: time.Now()}).(Transaction)
	apply(control, Action{Operation: "participants", Txn: txn.ID, Sequence: txn.Sequence, Participants: []uint8{0, 1}})
	prepared := make([]Prepared, 2)
	for i, s := range data {
		apply(s, Action{Operation: "reserve", Partition: uint8(i), Epoch: 1})
		changes := []Row{{Key: Key("entity", fmt.Sprintf("v%d", i)), Value: json.RawMessage(`{"kind":"host"}`)}}
		encoded, _ := json.Marshal(changes)
		prepared[i] = apply(s, Action{Operation: "prepare", Partition: uint8(i), Epoch: 1, Txn: txn.ID, Sequence: txn.Sequence, Changes: changes, Digest: fmt.Sprintf("%x", sha256.Sum256(encoded))}).(Prepared)
		if i == 0 {
			apply(control, Action{Operation: "prepared", Txn: txn.ID, Sequence: txn.Sequence, Partition: 0, Prepared: &prepared[0]})
			_, err := control.Apply(ctx, Action{Operation: "commit", Tenant: "t", Generation: 1, Txn: txn.ID, Sequence: txn.Sequence, Result: &storage.CommitResult{Manifest: storage.Manifest{Version: 1}}})
			if !errors.Is(err, storage.ErrConflict) {
				t.Fatalf("partial commit: %v", err)
			}
			before, err := control.GetGraph(ctx, "t")
			if err != nil || before.Version != 0 || before.Roots[0].Hash != "" {
				t.Fatalf("prepare became visible: %+v %v", before, err)
			}
		}
	}
	// Reopen the coordinator from its durable state rather than retaining any
	// in-memory transaction plan, as a newly elected leader must do.
	control = Store{Objects: controlObjects, Prefix: "graphdb"}
	apply(control, Action{Operation: "prepared", Txn: txn.ID, Sequence: txn.Sequence, Partition: 1, Prepared: &prepared[1]})
	apply(control, Action{Operation: "commit", Txn: txn.ID, Sequence: txn.Sequence, Result: &storage.CommitResult{Manifest: storage.Manifest{Version: 1}}})
	_, err := control.Apply(ctx, Action{Operation: "abort", Tenant: "t", Generation: 1, Txn: txn.ID, Sequence: txn.Sequence})
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("committed transaction aborted: %v", err)
	}
	after, err := control.GetGraph(ctx, "t")
	if err != nil || after.Version != 1 || after.Active.State != "committed" {
		t.Fatalf("publication lost: %+v %v", after, err)
	}
	for i, s := range data {
		state, err := s.Data(ctx, "t", 1, uint8(i))
		if err != nil || state.Root.Hash != "" || state.Prepared == nil {
			t.Fatalf("participant state: %+v %v", state, err)
		}
		key := Key("entity", fmt.Sprintf("v%d", i))
		_, found, err := s.Tree("t", 1, uint8(i)).Get(ctx, after.Roots[i], key)
		if err != nil || !found {
			t.Fatalf("published root not durable: %v %v", found, err)
		}
		apply(s, Action{Operation: "finalize", Partition: uint8(i), Epoch: 1, Txn: txn.ID, Sequence: txn.Sequence, Prepared: &prepared[i]})
		apply(s, Action{Operation: "finalize", Partition: uint8(i), Epoch: 1, Txn: txn.ID, Sequence: txn.Sequence, Prepared: &prepared[i]})
	}
	apply(control, Action{Operation: "complete", Txn: txn.ID, Sequence: txn.Sequence})
	replay := apply(control, Action{Operation: "begin", Txn: "different", Identity: "key", Digest: digest, Inputs: []Input{ref}, At: time.Now()}).(Transaction)
	if replay.ID != txn.ID || replay.State != "committed" || replay.Result.Version != 1 {
		t.Fatalf("idempotency lost: %+v", replay)
	}
	changed := digest[:63] + "0"
	if changed == digest {
		changed = digest[:63] + "1"
	}
	_, err = control.Apply(ctx, Action{Operation: "begin", Tenant: "t", Generation: 1, Txn: "different", Identity: "key", Digest: changed, Inputs: []Input{ref}, At: time.Now()})
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("idempotency content conflict lost: %v", err)
	}
}
