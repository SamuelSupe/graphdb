package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"slices"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (s Store) applyControl(ctx context.Context, a Action) (any, error) {
	g, err := s.GetGraph(ctx, a.Tenant)
	if a.Operation == "initialize" {
		if err == nil {
			return nil, conflict("graph control state already exists")
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if a.Graph == nil || a.Graph.Tenant != a.Tenant || a.Graph.Generation != a.Generation || a.Graph.Format != FormatVersion || a.Graph.Version < 0 || a.Graph.Active != nil {
			return nil, invalid("invalid initial graph descriptor")
		}
		g = *a.Graph
		for _, p := range g.Partitions {
			if p.Group == "" || p.Epoch == 0 {
				return nil, invalid("all graph partitions need an owner")
			}
		}
		g.State = "initializing"
		if err := s.save(ctx, s.graphKey(a.Tenant), g); err != nil {
			return nil, err
		}
		tenants, err := s.GraphTenants(ctx)
		if err != nil {
			return nil, err
		}
		found := false
		for _, tenant := range tenants {
			found = found || tenant == a.Tenant
		}
		if !found {
			tenants = append(tenants, a.Tenant)
		}
		return g, s.save(ctx, path.Join(s.Prefix, "control/graphs/registry.json"), tenants)
	}
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("required graph control state is missing")
		}
		return nil, err
	}
	if g.Generation != a.Generation {
		return nil, conflict("graph generation changed")
	}
	if strings.HasPrefix(a.Operation, "graph_gc_") {
		return s.applyGraphGC(ctx, g, a)
	}
	if strings.HasPrefix(a.Operation, "graph_move_") {
		return s.applyGraphMove(ctx, g, a)
	}
	if a.Operation == "source_retired" {
		if g.State != "active" {
			return nil, conflict("source cleanup requires an active graph")
		}
		g.SourceRetired = true
		g.SourceCleanupRevision = a.Revision
		return g, s.save(ctx, s.graphKey(g.Tenant), g)
	}
	if a.Operation == "activate" {
		if g.State == "active" {
			return g, nil
		}
		if g.State != "initializing" || g.Active != nil {
			return nil, conflict("graph initialization is not complete")
		}
		if g.Copy != nil && (g.Copy.Phase != "verify" || g.Copy.DataHash != g.DataHash) {
			return nil, conflict("copied graph does not match its source logical digest")
		}
		g.State = "active"
		g.Copy = nil
		if a.Revision == 0 {
			if err := s.saveView(ctx, g); err != nil {
				return nil, err
			}
		}
		return g, s.save(ctx, s.graphKey(g.Tenant), g)
	}
	if a.Operation == "import_identities" {
		if g.State != "initializing" || g.Active != nil || len(a.Transactions) > 128 {
			return nil, conflict("graph identities can only be imported into an idle unpublished generation")
		}
		encoded, err := json.Marshal(a.Transactions)
		if err != nil {
			return nil, err
		}
		if len(encoded) > MaxChangeBytes {
			return nil, invalid("identity import exceeds its byte budget")
		}
		for _, txn := range a.Transactions {
			if txn.Tenant != g.Tenant || txn.Generation != g.Generation || txn.Purpose != "legacy" || txn.State != "committed" && txn.State != "retryable" || len(txn.Identity) > 512 || txn.Result.Version > g.Version || !validDigest(txn.Digest) {
				return nil, invalid("invalid imported commit identity")
			}
			previous, err := s.Transaction(ctx, g.Tenant, g.Generation, txn.Identity)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			if err == nil && !equalJSON(previous, txn) {
				return nil, conflict("imported commit identity changed")
			}
		}
		for _, txn := range a.Transactions {
			if err := s.save(ctx, s.txnKey(g.Tenant, g.Generation, txn.Identity), txn); err != nil {
				return nil, err
			}
		}
		return map[string]int{"imported": len(a.Transactions)}, nil
	}
	if a.Operation == "copy_progress" {
		if g.State != "initializing" || g.Copy == nil || g.Active != nil || g.Copy.Phase != a.Purpose || g.Copy.Cursor != a.ExpectedCursor {
			return nil, conflict("graph copy checkpoint changed")
		}
		if a.Phase != "schema" && a.Phase != "entities" && a.Phase != "edges" && a.Phase != "identities" && a.Phase != "verify" || len(a.Cursor) > 16384 {
			return nil, invalid("invalid graph copy checkpoint")
		}
		g.Copy.Phase = a.Phase
		g.Copy.Cursor = a.Cursor
		return g, s.save(ctx, s.graphKey(g.Tenant), g)
	}
	if a.Operation == "pin" {
		if a.Revision == 0 {
			return s.createLegacyPin(ctx, g, a)
		}
		return s.createPin(ctx, g, a)
	}
	if a.Operation == "release_pin" {
		if !validTokenID(a.Txn) {
			return nil, invalid("invalid graph pin")
		}
		key := s.pinKey(g.Tenant, g.Generation, a.Txn)
		var pin Pin
		err := s.load(ctx, key, &pin)
		if errors.Is(err, storage.ErrNotFound) {
			return map[string]bool{"released": true}, nil
		}
		if err != nil {
			return nil, err
		}
		if pin.Query == "" || pin.Query != a.Digest {
			return nil, conflict("graph pin belongs to a different query")
		}
		return map[string]bool{"released": true}, s.Objects.Delete(ctx, key)
	}
	if a.Operation == "begin" {
		if a.Purpose == "" {
			a.Purpose = "commit"
		}
		if a.Purpose != "commit" && a.Purpose != "build" && a.Purpose != "ingest" || a.Purpose == "build" && g.State != "initializing" {
			return nil, invalid("invalid graph transaction purpose")
		}
		if a.Identity == "" || len(a.Identity) > 512 || a.Txn == "" || !validDigest(a.Digest) || a.At.IsZero() || len(a.Inputs) == 0 || len(a.Inputs) > 16 {
			return nil, invalid("invalid graph transaction identity or inputs")
		}
		previous, err := s.Transaction(ctx, a.Tenant, a.Generation, a.Identity)
		if err == nil {
			if previous.Digest != a.Digest {
				return nil, conflict("idempotency identity was used with different content")
			}
			if previous.State != "retryable" {
				return previous, nil
			}
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if g.GC.BlocksWrites() || g.Move.BlocksWrites() || g.Active != nil || g.Version != a.BaseVersion || g.Sequence == math.MaxUint64 {
			return nil, conflict("graph version or active transaction changed")
		}
		if a.ExpectedVersion != nil && *a.ExpectedVersion != g.Version {
			return nil, conflict("expected graph version does not match")
		}
		if g.State != "active" && g.State != "initializing" {
			return nil, conflict("graph is not active")
		}
		var participants []uint8
		if a.Revision >= BatchGCRevision && len(a.Participants) != 0 {
			if a.Purpose != "ingest" || a.LayoutRevision != g.LayoutRevision {
				return nil, conflict("preplanned ingest layout changed")
			}
			if err := validateParticipants(a.Participants); err != nil {
				return nil, err
			}
			participants = a.Participants
		}
		for _, ref := range a.Inputs {
			if ref.Stage != a.Txn {
				return nil, invalid("transaction input belongs to a different staging session")
			}
			if ref.Acceptance != "" && (a.Purpose != "ingest" || len(a.Inputs) != 1 || ref.Digest != a.Digest) {
				return nil, invalid("invalid ingest acceptance reference")
			}
			if _, err := s.Input(ctx, a.Tenant, a.Generation, ref); err != nil {
				if a.Revision >= InputGCRevision && ref.Acceptance == "" && errors.Is(err, errInputMissing) {
					return nil, ErrInputExpired
				}
				return nil, err
			}
		}
		g.Sequence++
		g.Active = &Transaction{Purpose: a.Purpose, Tenant: g.Tenant, Generation: g.Generation, ID: a.Txn, Identity: a.Identity, Sequence: g.Sequence, Digest: a.Digest, Inputs: a.Inputs, At: a.At, BaseVersion: g.Version, State: "preparing", Prepared: map[uint8]Prepared{}, Participants: participants}
		return *g.Active, s.saveGraphTransaction(ctx, g)
	}
	if g.Active == nil || g.Active.ID != a.Txn || g.Active.Sequence != a.Sequence {
		return nil, conflict("active graph transaction changed")
	}
	txn := g.Active
	switch a.Operation {
	case "participants":
		if txn.State != "preparing" || len(a.Participants) == 0 {
			return nil, conflict("transaction cannot change participants")
		}
		if len(txn.Participants) > 0 {
			old, _ := json.Marshal(txn.Participants)
			next, _ := json.Marshal(a.Participants)
			if string(old) != string(next) {
				return nil, conflict("participant set is immutable")
			}
		}
		if err := validateParticipants(a.Participants); err != nil {
			return nil, err
		}
		txn.Participants = a.Participants
	case "prepared":
		if a.Prepared == nil {
			return nil, conflict("invalid prepare result")
		}
		if err := registerPrepared(&g, txn, a.Partition, *a.Prepared); err != nil {
			return nil, err
		}
	case "commit":
		if txn.State == "committed" {
			return *txn, nil
		}
		for i := range g.Partitions {
			if p, ok := a.PreparedBatch[uint8(i)]; ok {
				if err := registerPrepared(&g, txn, uint8(i), p); err != nil {
					return nil, err
				}
			}
		}
		if txn.State != "preparing" || len(txn.Participants) == 0 || len(txn.Prepared) != len(txn.Participants) || a.Result == nil || g.Version != txn.BaseVersion {
			return nil, conflict("transaction is not fully prepared")
		}
		if txn.Purpose == "ingest" && a.IngestResult == nil {
			return nil, invalid("ingest publication requires its terminal result")
		}
		if a.Result.Version != g.Version && a.Result.Version != g.Version+1 || g.Version == math.MaxInt64 {
			return nil, conflict("invalid graph publication version")
		}
		for _, id := range txn.Participants {
			p, ok := txn.Prepared[id]
			if !ok {
				return nil, conflict("participant prepare is missing")
			}
			g.Roots[id] = p.Root
			g.EntityCount += p.EntityDelta
			g.EdgeCount += p.EdgeDelta
			if g.EdgeTypeCounts == nil {
				g.EdgeTypeCounts = map[string]int64{}
			}
			for kind, delta := range p.EdgeTypeDelta {
				g.EdgeTypeCounts[kind] += delta
				if g.EdgeTypeCounts[kind] < 0 {
					return nil, invalid("negative relation edge count")
				}
			}
			g.EntityCounts[id] += p.EntityDelta
			if g.EntityCounts[id] < 0 {
				return nil, conflict("negative partition entity count")
			}
			for bucket, digest := range p.HashBuckets {
				if bucket < 0 || bucket >= len(g.HashBuckets) || digest != "" && !validDigest(digest) {
					return nil, invalid("invalid logical hash bucket")
				}
				g.HashBuckets[bucket] = digest
			}
		}
		if g.Version != a.Result.Version {
			g.ReadLease = nil
		}
		g.Version = a.Result.Version
		if g.EntityCount < 0 || g.EdgeCount < 0 {
			return nil, invalid("negative graph record count")
		}
		digest, err := graph.ContentHashFromBuckets(g.HashBuckets)
		if err != nil {
			return nil, err
		}
		if a.Result.DataHash != "" && a.Result.DataHash != digest {
			return nil, conflict("logical graph digest does not match prepared buckets")
		}
		a.Result.DataHash = digest
		g.DataHash = digest
		g.CITypes = a.CITypes
		g.RelationTypes = a.RelationTypes
		txn.State = "committed"
		txn.Result = *a.Result
		txn.IngestResult = a.IngestResult
		if a.Revision == 0 {
			if err := s.saveView(ctx, g); err != nil {
				return nil, err
			}
		}
	case "abort":
		if txn.State == "committed" {
			return nil, conflict("committed graph transactions cannot abort")
		}
		if txn.Purpose == "ingest" && a.IngestResult == nil {
			return nil, invalid("ingest abort requires its terminal result")
		}
		txn.State = "aborted"
		txn.Error = a.Error
		txn.ErrorCode = a.ErrorCode
		txn.IngestResult = a.IngestResult
	case "complete":
		if txn.State != "committed" && txn.State != "aborted" {
			return nil, conflict("transaction is undecided")
		}
		inputs := txn.Inputs
		if a.ReceiptRef != "" && (txn.Purpose != "ingest" || len(inputs) != 1 || inputs[0].Acceptance != a.ReceiptRef || txn.IngestResult == nil) {
			return nil, conflict("graph completion receipt does not match its input")
		}
		if err := s.journalObjects(ctx, s.graphKey(g.Tenant), s.txnKey(g.Tenant, g.Generation, txn.Identity)); err != nil {
			return nil, err
		}
		txn.Inputs = nil
		txn.Prepared = nil
		txn.Participants = nil
		if a.ReceiptRef == "" {
			if err := s.save(ctx, s.txnKey(a.Tenant, a.Generation, txn.Identity), txn); err != nil {
				return nil, err
			}
		}
		for _, input := range inputs {
			if input.Acceptance != "" {
				continue // The WAL receipt owns this input until queue completion.
			}
			if err := s.Objects.Delete(ctx, s.inputKey(g.Tenant, g.Generation, input.Stage, input.Digest)); err != nil {
				return nil, err
			}
		}
		g.Active = nil
		if a.Revision == 0 && txn.State == "committed" {
			if err := s.saveView(ctx, g); err != nil {
				return nil, err
			}
		}

		return *txn, s.save(ctx, s.graphKey(a.Tenant), g)
	default:
		return nil, invalid("unsupported graph action")
	}
	return *txn, s.saveGraphTransaction(ctx, g)
}

func (s Store) saveGraphTransaction(ctx context.Context, g Graph) error {
	graphKey, txnKey := s.graphKey(g.Tenant), s.txnKey(g.Tenant, g.Generation, g.Active.Identity)
	if err := s.journalObjects(ctx, graphKey, txnKey); err != nil {
		return err
	}
	if err := s.save(ctx, txnKey, g.Active); err != nil {
		return err
	}
	return s.save(ctx, graphKey, g)
}

func validateParticipants(ids []uint8) error {
	var seen [256]bool
	for _, id := range ids {
		if seen[id] {
			return invalid("duplicate participant")
		}
		seen[id] = true
	}
	return nil
}

func registerPrepared(g *Graph, txn *Transaction, id uint8, p Prepared) error {
	if txn.State != "preparing" || p.Txn != txn.ID || p.Sequence != txn.Sequence || p.Base != g.Roots[id] {
		return conflict("invalid prepare result")
	}
	if !slices.Contains(txn.Participants, id) {
		return conflict("unregistered transaction participant")
	}
	if old, ok := txn.Prepared[id]; ok && !samePrepared(old, p) {
		return conflict("prepare result changed")
	}
	if txn.Prepared == nil {
		txn.Prepared = map[uint8]Prepared{}
	}
	txn.Prepared[id] = p
	return nil
}

func (s Store) GraphTenants(ctx context.Context) ([]string, error) {
	var tenants []string
	err := s.load(ctx, path.Join(s.Prefix, "control/graphs/registry.json"), &tenants)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return tenants, err
}
