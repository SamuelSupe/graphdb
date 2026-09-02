package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatedIngestBatchesRebaseAcrossWriters(t *testing.T) {
	for _, writerCount := range []int{2, 4, 8} {
		writerCount := writerCount
		t.Run(fmt.Sprintf("%d-writers", writerCount), func(t *testing.T) {
			ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, fmt.Sprintf("ingest-rebase-%d", writerCount))
			dsn := postgresTestDSN(t)
			coordinators := make([]*PostgresCoordinator, 0, writerCount-1)
			t.Cleanup(func() {
				for _, coordinator := range coordinators {
					coordinator.Close()
				}
			})
			objects := NewMemoryStore()
			stores := make([]*TenantStore, writerCount)
			stores[0] = NewTenantStore(objects, "test")
			stores[0].InstanceID = "writer-0"
			stores[0].CoordinatorRetryLimit = 32
			stores[0].SetCoordinator(firstCoordinator)
			for index := 1; index < writerCount; index++ {
				coordinator, err := NewPostgresCoordinator(ctx, dsn, firstCoordinator.schema, firstCoordinator.namespace)
				if err != nil {
					t.Fatalf("new coordinator %d: %v", index, err)
				}
				coordinators = append(coordinators, coordinator)
				stores[index] = NewTenantStore(objects, "test")
				stores[index].InstanceID = fmt.Sprintf("writer-%d", index)
				stores[index].CoordinatorRetryLimit = 32
				stores[index].SetCoordinator(coordinator)
			}
			if _, err := stores[0].CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
				t.Fatalf("create coordinated tenant: %v", err)
			}

			requests := make([]IngestRequest, writerCount)
			for index := range requests {
				requests[index] = IngestRequest{
					Source: "agent", CollectorID: fmt.Sprintf("collector-%d", index), BatchID: fmt.Sprintf("batch-%d", index),
					Items: []IngestItem{{ExternalID: fmt.Sprintf("host:%d", index), Entity: &graph.Entity{ID: fmt.Sprintf("host:%d", index), Kind: "host"}}},
				}
			}
			start := make(chan struct{})
			results := make(chan struct {
				result IngestResult
				err    error
			}, writerCount)
			var wait sync.WaitGroup
			for index, store := range stores {
				index, store := index, store
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					result, err := ingestDurableWithExplicitRetry(ctx, store, "tenant-a", IngestBatchEntry{
						Request: requests[index], AcceptedAt: time.Now().UTC(),
					})
					results <- struct {
						result IngestResult
						err    error
					}{result: result, err: err}
				}()
			}
			close(start)
			wait.Wait()
			close(results)
			for outcome := range results {
				if outcome.err != nil {
					t.Fatalf("concurrent coordinated ingest: %v", outcome.err)
				}
				if outcome.result.Failed != 0 || outcome.result.Applied != 1 || outcome.result.Version <= 0 {
					t.Fatalf("coordinated ingest result = %#v", outcome.result)
				}
			}

			head, exists, err := firstCoordinator.Head(ctx, "tenant-a")
			if err != nil || !exists {
				t.Fatalf("coordinated head exists=%v err=%v", exists, err)
			}
			if head.GraphVersion != int64(writerCount) {
				t.Fatalf("coordinated head = %#v, want %d rebased versions", head, writerCount)
			}
			loaded, _, err := stores[0].Load(ctx, "tenant-a")
			if err != nil {
				t.Fatalf("load rebased graph: %v", err)
			}
			for index := range requests {
				id := fmt.Sprintf("host:%d", index)
				if _, ok := loaded.GetEntity(id); !ok {
					t.Fatalf("rebased graph is missing %s", id)
				}
			}
		})
	}
}

func TestPostgresCoordinatedIngestCASCohortPublishesOneManifestAndCompletesMetadata(t *testing.T) {
	ctx, postgres := newPostgresIntegrationCoordinator(t, "ingest-cas-cohort")
	counting := &countingPostgresIngestCoordinator{PostgresCoordinator: postgres}
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "cohort-writer"
	store.CoordinatorRetryLimit = 32
	store.SetCoordinator(counting)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	expected := int64(0)
	requests := []IngestRequest{
		{
			Source: "agent", CollectorID: "collector-cas", BatchID: "pg-cas-1", Cursor: "cursor-1",
			ExpectedVersion: &expected,
			Items: []IngestItem{{
				ExternalID: "shared",
				Entity:     &graph.Entity{ID: "host:shared", Kind: "host", Fields: graph.Fields{"owner": "first", "sequence": 1}},
			}},
		},
		{
			Source: "agent", CollectorID: "collector-cas", BatchID: "pg-cas-2", Cursor: "cursor-2",
			ExpectedVersion: &expected,
			Items: []IngestItem{{
				ExternalID: "shared",
				Entity:     &graph.Entity{ID: "host:shared", Kind: "host", Fields: graph.Fields{"owner": "second", "sequence": 2}},
			}, {
				ExternalID: "host-2",
				Entity:     &graph.Entity{ID: "host:2", Kind: "host"},
			}},
		},
	}
	entries := []IngestBatchEntry{
		{Request: requests[0], AcceptedAt: time.Unix(10, 0).UTC()},
		{Request: requests[1], AcceptedAt: time.Unix(11, 0).UTC()},
	}
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(ctx, "tenant-a", entries, IngestBatchHooks{
		Stats: func(got IngestBatchStats) {
			stats = got
		},
	})
	if err != nil {
		t.Fatalf("coordinated CAS cohort: %v", err)
	}
	if len(results) != len(requests) {
		t.Fatalf("cohort results = %#v, want %d results", results, len(requests))
	}
	if results[0].Version != 1 || results[0].Applied != 1 || results[0].Failed != 0 || results[0].ErrorCode != "" {
		t.Fatalf("first cohort result = %#v, want version 1 success", results[0])
	}
	if results[1].Version != 2 || results[1].Applied != 2 || results[1].Failed != 0 || results[1].ErrorCode != "" {
		t.Fatalf("second cohort result = %#v, want version 2 success", results[1])
	}
	if stats.LogicalCommits != 2 || stats.CASMerged != 2 || stats.Fallback || stats.Segments != 1 || stats.ManifestPublishes != 1 {
		t.Fatalf("cohort stats = %#v, want two merged logical commits, one segment/manifest and no fallback", stats)
	}

	head, exists, err := postgres.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("cohort head exists=%v err=%v", exists, err)
	}
	if head.GraphVersion != 2 || head.Revision != 2 {
		t.Fatalf("cohort head = %#v, want graph version 2 and revision 2", head)
	}
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load cohort graph: %v", err)
	}
	if manifest.Version != 2 || loaded.Version != 2 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("cohort graph/manifest = version %d/%d segments %#v, want one two-commit segment", loaded.Version, manifest.Version, manifest.CommitSegments)
	}
	shared, ok := loaded.GetEntity("host:shared")
	if !ok || shared.Fields["owner"] != "second" || shared.Fields["sequence"] != float64(2) {
		t.Fatalf("shared entity = %#v, want WAL-later value", shared)
	}
	if _, ok := loaded.GetEntity("host:2"); !ok {
		t.Fatal("second cohort entity is not visible")
	}
	if calls := counting.publishBatchCallCount(); calls != 1 {
		t.Fatalf("PublishIngestBatch calls = %d, want exactly one", calls)
	}

	for _, request := range requests {
		record, err := store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
		if err != nil {
			t.Fatalf("load ingest metadata for %s: %v", request.BatchID, err)
		}
		if record.Result.Version == 0 || record.Result.Failed != 0 {
			t.Fatalf("ingest metadata %s = %#v, want committed result", request.BatchID, record.Result)
		}
		key := coordinatedIngestIdempotencyKey(request)
		reservation, err := postgres.loadCommitReservation(ctx, "tenant-a", key)
		if err != nil {
			t.Fatalf("load committed reservation %s: %v", key, err)
		}
		if !reservation.Committed {
			t.Fatalf("reservation %s = %#v, want committed", key, reservation)
		}
		if strings.Contains(string(reservation.Result), `"mutations"`) {
			t.Fatalf("PostgreSQL reservation %s persisted graph/WAL payload: %s", key, reservation.Result)
		}
	}
	collector, exists, err := postgres.CollectorState(ctx, "tenant-a", "agent", "collector-cas")
	if err != nil || !exists {
		t.Fatalf("collector state exists=%v err=%v", exists, err)
	}
	if collector.BatchID != "pg-cas-2" || collector.Cursor != "cursor-2" || collector.Version != 2 {
		t.Fatalf("collector state = %#v, want final cohort cursor/version", collector)
	}
}

func TestPostgresCoordinatedIngestCASCohortUsesCommonPreconditionsAroundAtomicBarrier(t *testing.T) {
	ctx, postgres := newPostgresIntegrationCoordinator(t, "ingest-cas-barrier")
	counting := &countingPostgresIngestCoordinator{PostgresCoordinator: postgres}
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "barrier-writer"
	store.CoordinatorRetryLimit = 32
	store.SetCoordinator(counting)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	expected := int64(0)
	entries := []IngestBatchEntry{
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-barrier", BatchID: "barrier-cohort-1", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "base", Entity: &graph.Entity{ID: "host:base", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-barrier", BatchID: "barrier-cohort-2", ExpectedVersion: &expected,
			Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:base", Op: "not_exists"}},
			Items:         []IngestItem{{ExternalID: "cohort-2", Entity: &graph.Entity{ID: "host:cohort-2", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-barrier", BatchID: "barrier-cohort-3", ExpectedVersion: &expected,
			Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:base", Op: "exists"}},
			Items:         []IngestItem{{ExternalID: "cohort-3", Entity: &graph.Entity{ID: "host:cohort-3", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-barrier", BatchID: "barrier-atomic", FailureMode: IngestFailureModeAtomic,
			Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:missing", Op: "exists"}},
			Items:         []IngestItem{{ExternalID: "atomic", Entity: &graph.Entity{ID: "host:atomic", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-barrier", BatchID: "barrier-after",
			Items: []IngestItem{{ExternalID: "ordinary", Entity: &graph.Entity{ID: "host:ordinary", Kind: "host"}}},
		}},
	}
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(ctx, "tenant-a", entries, IngestBatchHooks{
		Stats: func(got IngestBatchStats) {
			stats = got
		},
	})
	if err != nil {
		t.Fatalf("coordinated CAS barrier batch: %v", err)
	}
	if len(results) != len(entries) {
		t.Fatalf("barrier results = %#v, want %d results", results, len(entries))
	}
	wantResults := []IngestResult{
		{Version: 1, Applied: 1},
		{Version: 2, Applied: 1},
		{Version: 0, Failed: 1, ErrorCode: IngestErrorPreconditionFailed},
		{Version: 0, Failed: 1, ErrorCode: IngestErrorPreconditionFailed},
		{Version: 3, Applied: 1},
	}
	for index, result := range results {
		want := wantResults[index]
		if result.Version != want.Version || result.Applied != want.Applied || result.Failed != want.Failed || result.ErrorCode != want.ErrorCode {
			t.Fatalf("barrier result %d = %#v, want version=%d applied=%d failed=%d code=%q", index, result, want.Version, want.Applied, want.Failed, want.ErrorCode)
		}
	}
	if calls := counting.publishBatchCallCount(); calls != 1 {
		t.Fatalf("barrier PublishIngestBatch calls = %d, want one", calls)
	}
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load barrier graph: %v", err)
	}
	if manifest.Version != 3 || loaded.Version != 3 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 3 {
		t.Fatalf("barrier graph/manifest = version %d/%d segments %#v, want three logical versions in one segment", loaded.Version, manifest.Version, manifest.CommitSegments)
	}
	for _, id := range []string{"host:base", "host:cohort-2", "host:ordinary"} {
		if _, ok := loaded.GetEntity(id); !ok {
			t.Fatalf("barrier entity %q is missing", id)
		}
	}
	if _, ok := loaded.GetEntity("host:cohort-3"); ok {
		t.Fatal("failed common-base precondition request was published")
	}
	if _, ok := loaded.GetEntity("host:atomic"); ok {
		t.Fatal("failed atomic barrier request was published")
	}
	if stats.LogicalCommits != 3 || stats.CASMerged != 2 || stats.Segments != 1 || stats.ManifestPublishes != 1 {
		t.Fatalf("barrier stats = %#v, want three logical commits, two merged cohort members and one segment/manifest", stats)
	}
}

func TestPostgresCoordinatedIngestCASCohortsCompeteByHeadCAS(t *testing.T) {
	ctx, firstPostgres := newPostgresIntegrationCoordinator(t, "ingest-cas-race")
	secondPostgres, err := NewPostgresCoordinator(ctx, postgresTestDSN(t), firstPostgres.schema, firstPostgres.namespace)
	if err != nil {
		t.Fatalf("new second coordinator: %v", err)
	}
	t.Cleanup(secondPostgres.Close)
	raceGate := newIngestCASCASRaceGate()
	first := &ingestCASCASRaceCoordinator{PostgresCoordinator: firstPostgres, gate: raceGate}
	second := &ingestCASCASRaceCoordinator{PostgresCoordinator: secondPostgres, gate: raceGate}
	objects := NewMemoryStore()
	firstStore := NewTenantStore(objects, "test")
	firstStore.InstanceID = "writer-a"
	firstStore.CoordinatorRetryLimit = 32
	firstStore.LeaseTTL = time.Hour
	firstStore.SetCoordinator(first)
	secondStore := NewTenantStore(objects, "test")
	secondStore.InstanceID = "writer-b"
	secondStore.CoordinatorRetryLimit = 32
	secondStore.LeaseTTL = time.Hour
	secondStore.SetCoordinator(second)
	if _, err := firstStore.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	makeCohort := func(prefix string) []IngestBatchEntry {
		expected := int64(0)
		return []IngestBatchEntry{
			{Request: IngestRequest{
				Source: "agent", CollectorID: "collector-race", BatchID: prefix + "-1", Cursor: prefix + "-cursor-1", ExpectedVersion: &expected,
				Items: []IngestItem{{ExternalID: prefix + "-shared", Entity: &graph.Entity{ID: "host:race-shared", Kind: "host", Fields: graph.Fields{"winner": prefix, "sequence": 1}}}},
			}},
			{Request: IngestRequest{
				Source: "agent", CollectorID: "collector-race", BatchID: prefix + "-2", Cursor: prefix + "-cursor-2", ExpectedVersion: &expected,
				Items: []IngestItem{{ExternalID: prefix + "-unique", Entity: &graph.Entity{ID: "host:race-" + prefix, Kind: "host"}}},
			}},
		}
	}

	start := make(chan struct{})
	type outcome struct {
		writer  string
		results []IngestResult
		err     error
	}
	outcomes := make(chan outcome, 2)
	var wait sync.WaitGroup
	for _, item := range []struct {
		name  string
		store *TenantStore
		batch []IngestBatchEntry
	}{
		{name: "writer-a", store: firstStore, batch: makeCohort("a")},
		{name: "writer-b", store: secondStore, batch: makeCohort("b")},
	} {
		item := item
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			results, err := ingestCohortWithExplicitRetry(ctx, item.store, "tenant-a", item.batch)
			outcomes <- outcome{writer: item.name, results: results, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(outcomes)

	var winner, loser outcome
	for current := range outcomes {
		if current.err != nil {
			t.Fatalf("%s CAS cohort: %v", current.writer, current.err)
		}
		if len(current.results) != 2 {
			t.Fatalf("%s results = %#v, want two results", current.writer, current.results)
		}
		success := current.results[0].Applied == 1 && current.results[0].Failed == 0 && current.results[0].Version == 1 &&
			current.results[1].Applied == 1 && current.results[1].Failed == 0 && current.results[1].Version == 2
		conflict := current.results[0].Applied == 0 && current.results[0].Failed == 1 && current.results[0].Version == 0 && current.results[0].ErrorCode == IngestErrorVersionConflict &&
			current.results[1].Applied == 0 && current.results[1].Failed == 1 && current.results[1].Version == 0 && current.results[1].ErrorCode == IngestErrorVersionConflict
		switch {
		case success:
			if winner.writer != "" {
				t.Fatalf("both writers reported successful cohorts: %#v and %#v", winner, current)
			}
			winner = current
		case conflict:
			if loser.writer != "" {
				t.Fatalf("both writers reported terminal conflict cohorts: %#v and %#v", loser, current)
			}
			loser = current
		default:
			t.Fatalf("unexpected %s cohort results = %#v", current.writer, current.results)
		}
	}
	if winner.writer == "" || loser.writer == "" {
		t.Fatalf("winner/loser classification = %#v/%#v", winner, loser)
	}

	head, exists, err := firstPostgres.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("final head exists=%v err=%v", exists, err)
	}
	if head.GraphVersion != 2 || head.Revision != 2 {
		t.Fatalf("final head = %#v, want only winner's two versions", head)
	}
	loaded, manifest, err := firstStore.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load final race graph: %v", err)
	}
	if manifest.Version != 2 || loaded.Version != 2 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("final graph/manifest = version %d/%d segments %#v, want one two-version winner cohort", loaded.Version, manifest.Version, manifest.CommitSegments)
	}
	for _, id := range []string{"host:race-shared", "host:race-" + string(winner.writer[len(winner.writer)-1])} {
		if _, ok := loaded.GetEntity(id); !ok {
			t.Fatalf("winner entity %q is missing", id)
		}
	}
	loserSuffix := string(loser.writer[len(loser.writer)-1])
	if _, ok := loaded.GetEntity("host:race-" + loserSuffix); ok {
		t.Fatalf("loser entity host:race-%s was published", loserSuffix)
	}
	shared, ok := loaded.GetEntity("host:race-shared")
	if !ok || shared.Fields["winner"] != winner.writer[len(winner.writer)-1:] {
		t.Fatalf("shared winner field = %#v, want writer %s", shared.Fields, winner.writer)
	}

	collector, exists, err := firstPostgres.CollectorState(ctx, "tenant-a", "agent", "collector-race")
	if err != nil || !exists {
		t.Fatalf("race collector state exists=%v err=%v", exists, err)
	}
	if collector.Version != 2 || collector.BatchID != winner.writer[len(winner.writer)-1:]+"-2" {
		t.Fatalf("race collector state = %#v, want winner's version-2 batch", collector)
	}
	if calls := raceGate.publishCallCount(); calls < 2 {
		t.Fatalf("coordinated PublishIngestBatch calls = %d, want at least one CAS race with two attempts", calls)
	}
	assertCoordinatorRows(t, firstPostgres,
		`SELECT count(*) FROM `+firstPostgres.table("commit_idempotency")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND status = 'committed'`,
		2,
	)
	assertCoordinatorRows(t, firstPostgres,
		`SELECT count(*) FROM `+firstPostgres.table("commit_idempotency")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND status = 'pending'`,
		0,
	)
	rows, err := firstPostgres.pool.Query(ctx,
		`SELECT result_json::text FROM `+firstPostgres.table("commit_idempotency")+` WHERE namespace = $1 AND tenant_id = 'tenant-a'`,
		firstPostgres.namespace,
	)
	if err != nil {
		t.Fatalf("query race idempotency results: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var resultJSON string
		if err := rows.Scan(&resultJSON); err != nil {
			t.Fatalf("scan race idempotency result: %v", err)
		}
		if strings.Contains(resultJSON, `"mutations"`) {
			t.Fatalf("PostgreSQL persisted race WAL/graph payload: %s", resultJSON)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate race idempotency results: %v", err)
	}
}

func TestPostgresCoordinatedIngestReservationIdentityIncludesGuards(t *testing.T) {
	ctx, postgres := newPostgresIntegrationCoordinator(t, "ingest-reservation-identity")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "identity-writer"
	store.SetCoordinator(postgres)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	newCandidate := func(request IngestRequest) *ingestBatchCandidate {
		prepared, err := PrepareIngestRequest("tenant-a", request)
		if err != nil {
			t.Fatalf("prepare request %s: %v", request.BatchID, err)
		}
		_, mutations, _ := buildIngestMutations(prepared)
		return &ingestBatchCandidate{request: prepared, started: time.Unix(20, 0).UTC(), mutations: mutations}
	}
	abort := func(reservation *directCommitReservation) {
		t.Helper()
		if reservation == nil {
			return
		}
		if err := store.abortDirectCommit(reservation, errors.New("test cleanup")); err != nil {
			t.Fatalf("abort test reservation %s: %v", reservation.key, err)
		}
	}

	baseExpected := int64(0)
	baseRequest := IngestRequest{
		Source: "agent", CollectorID: "collector-identity", BatchID: "identity-expected", ExpectedVersion: &baseExpected,
		Items: []IngestItem{{ExternalID: "identity", Entity: &graph.Entity{ID: "host:identity", Kind: "host"}}},
	}
	firstReservation, replay, err := store.reserveCoordinatedIngestCandidate(ctx, "tenant-a", newCandidate(baseRequest))
	if err != nil || replay != nil || firstReservation == nil {
		t.Fatalf("initial expected-version reservation = %#v replay=%#v err=%v", firstReservation, replay, err)
	}
	changedExpected := int64(1)
	changedRequest := baseRequest
	changedRequest.ExpectedVersion = &changedExpected
	if _, _, err := store.reserveCoordinatedIngestCandidate(ctx, "tenant-a", newCandidate(changedRequest)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed expected-version reservation err = %v, want ErrIdempotencyConflict", err)
	}
	abort(firstReservation)

	preconditionRequest := IngestRequest{
		Source: "agent", CollectorID: "collector-identity", BatchID: "identity-precondition",
		Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:identity", Op: "not_exists"}},
		Items:         []IngestItem{{ExternalID: "identity-precondition", Entity: &graph.Entity{ID: "host:identity", Kind: "host"}}},
	}
	preconditionReservation, replay, err := store.reserveCoordinatedIngestCandidate(ctx, "tenant-a", newCandidate(preconditionRequest))
	if err != nil || replay != nil || preconditionReservation == nil {
		t.Fatalf("initial precondition reservation = %#v replay=%#v err=%v", preconditionReservation, replay, err)
	}
	changedPrecondition := preconditionRequest
	changedPrecondition.Preconditions = []IngestPrecondition{{ResourceType: "entity", ID: "host:identity", Op: "exists"}}
	if _, _, err := store.reserveCoordinatedIngestCandidate(ctx, "tenant-a", newCandidate(changedPrecondition)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed precondition reservation err = %v, want ErrIdempotencyConflict", err)
	}
	abort(preconditionReservation)

	atomicRequest := IngestRequest{
		Source: "agent", CollectorID: "collector-identity", BatchID: "identity-failure-mode", FailureMode: IngestFailureModeAtomic,
		Items: []IngestItem{{ExternalID: "identity-atomic", Entity: &graph.Entity{ID: "host:identity-atomic", Kind: "host"}}},
	}
	atomicReservation, replay, err := store.reserveCoordinatedIngestCandidate(ctx, "tenant-a", newCandidate(atomicRequest))
	if err != nil || replay != nil || atomicReservation == nil {
		t.Fatalf("initial atomic reservation = %#v replay=%#v err=%v", atomicReservation, replay, err)
	}
	bestEffortRequest := atomicRequest
	bestEffortRequest.FailureMode = IngestFailureModeBestEffort
	if _, _, err := store.reserveCoordinatedIngestCandidate(ctx, "tenant-a", newCandidate(bestEffortRequest)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed failure-mode reservation err = %v, want ErrIdempotencyConflict", err)
	}
	abort(atomicReservation)

	noGuardRequest := IngestRequest{
		Source: "agent", CollectorID: "collector-identity", BatchID: "identity-replay",
		Items: []IngestItem{{ExternalID: "identity-replay", Entity: &graph.Entity{ID: "host:identity-replay", Kind: "host"}}},
	}
	if _, err := store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{Request: noGuardRequest}}); err != nil {
		t.Fatalf("publish no-guard request: %v", err)
	}
	explicitBestEffort := noGuardRequest
	explicitBestEffort.FailureMode = IngestFailureModeBestEffort
	results, err := store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{Request: explicitBestEffort}})
	if err != nil {
		t.Fatalf("replay no-guard request: %v", err)
	}
	if len(results) != 1 || !results[0].Skipped || results[0].SkipReason != IngestSkipReasonIdempotentReplay || results[0].Version != 1 {
		t.Fatalf("no-guard replay result = %#v, want compatible idempotent replay", results)
	}

	guardedReplay := atomicRequest
	guardedReplay.BatchID = "identity-guarded-replay"
	guardedReplay.FailureMode = IngestFailureModeAtomic
	guardedReplay.Preconditions = []IngestPrecondition{{ResourceType: "entity", ID: "host:identity-atomic", Op: "not_exists"}}
	if _, err := store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{Request: guardedReplay}}); err != nil {
		t.Fatalf("publish guarded request: %v", err)
	}
	results, err = store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{Request: guardedReplay}})
	if err != nil {
		t.Fatalf("replay guarded request: %v", err)
	}
	if len(results) != 1 || !results[0].Skipped || results[0].SkipReason != IngestSkipReasonIdempotentReplay {
		t.Fatalf("guarded replay result = %#v, want idempotent replay", results)
	}
}

func TestPostgresCoordinatedIngestBatchesKeepTenantHeadsIndependent(t *testing.T) {
	ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, "ingest-multi-tenant")
	dsn := postgresTestDSN(t)
	const tenantCount = 4
	coordinators := make([]*PostgresCoordinator, 0, tenantCount-1)
	t.Cleanup(func() {
		for _, coordinator := range coordinators {
			coordinator.Close()
		}
	})
	objects := NewMemoryStore()
	stores := make([]*TenantStore, tenantCount)
	stores[0] = NewTenantStore(objects, "test")
	stores[0].InstanceID = "tenant-writer-0"
	stores[0].CoordinatorRetryLimit = 32
	stores[0].SetCoordinator(firstCoordinator)
	for index := 1; index < tenantCount; index++ {
		coordinator, err := NewPostgresCoordinator(ctx, dsn, firstCoordinator.schema, firstCoordinator.namespace)
		if err != nil {
			t.Fatalf("new tenant coordinator %d: %v", index, err)
		}
		coordinators = append(coordinators, coordinator)
		stores[index] = NewTenantStore(objects, "test")
		stores[index].InstanceID = fmt.Sprintf("tenant-writer-%d", index)
		stores[index].CoordinatorRetryLimit = 32
		stores[index].SetCoordinator(coordinator)
	}
	for index := range stores {
		if _, err := stores[index].CreateTenant(ctx, fmt.Sprintf("tenant-%d", index), TenantCreateOptions{}); err != nil {
			t.Fatalf("create tenant %d: %v", index, err)
		}
	}

	start := make(chan struct{})
	results := make(chan struct {
		index  int
		result IngestResult
		err    error
	}, tenantCount)
	var wait sync.WaitGroup
	for index, store := range stores {
		index, store := index, store
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := ingestDurableWithExplicitRetry(ctx, store, fmt.Sprintf("tenant-%d", index), IngestBatchEntry{
				Request: IngestRequest{
					Source: "agent", CollectorID: "collector-independent", BatchID: fmt.Sprintf("batch-%d", index),
					Items: []IngestItem{{ExternalID: fmt.Sprintf("host:%d", index), Entity: &graph.Entity{ID: fmt.Sprintf("host:%d", index), Kind: "host"}}},
				},
			})
			results <- struct {
				index  int
				result IngestResult
				err    error
			}{index: index, result: result, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("concurrent tenant %d ingest: %v", outcome.index, outcome.err)
		}
		if outcome.result.Version != 1 || outcome.result.Applied != 1 || outcome.result.Failed != 0 {
			t.Fatalf("tenant %d result = %#v, want independent version 1", outcome.index, outcome.result)
		}
	}
	for index, store := range stores {
		tenantID := fmt.Sprintf("tenant-%d", index)
		head, exists, err := firstCoordinator.Head(ctx, tenantID)
		if err != nil || !exists || head.GraphVersion != 1 {
			t.Fatalf("head %s = %#v exists=%v err=%v, want independent version 1", tenantID, head, exists, err)
		}
		loaded, _, err := store.Load(ctx, tenantID)
		if err != nil {
			t.Fatalf("load %s: %v", tenantID, err)
		}
		if _, ok := loaded.GetEntity(fmt.Sprintf("host:%d", index)); !ok {
			t.Fatalf("tenant %s graph missing host:%d", tenantID, index)
		}
	}
}

func TestPostgresCoordinatedIngestCrossWriterIdempotency(t *testing.T) {
	ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, "ingest-idempotency")
	secondCoordinator, err := NewPostgresCoordinator(ctx, postgresTestDSN(t), firstCoordinator.schema, firstCoordinator.namespace)
	if err != nil {
		t.Fatalf("new second coordinator: %v", err)
	}
	t.Cleanup(secondCoordinator.Close)
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	first.InstanceID = "writer-a"
	first.CoordinatorRetryLimit = 32
	first.SetCoordinator(firstCoordinator)
	second := NewTenantStore(objects, "test")
	second.InstanceID = "writer-b"
	second.CoordinatorRetryLimit = 32
	second.SetCoordinator(secondCoordinator)
	if _, err := first.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}
	request := IngestRequest{
		Source: "agent", CollectorID: "collector-shared", BatchID: "batch-shared", IdempotencyKey: "idempotency-shared",
		Items: []IngestItem{{ExternalID: "host:shared", Entity: &graph.Entity{ID: "host:shared", Kind: "host"}}},
	}
	start := make(chan struct{})
	results := make(chan struct {
		result IngestResult
		err    error
	}, 2)
	var wait sync.WaitGroup
	for _, store := range []*TenantStore{first, second} {
		store := store
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := ingestDurableWithExplicitRetry(ctx, store, "tenant-a", IngestBatchEntry{Request: request})
			if err != nil {
				results <- struct {
					result IngestResult
					err    error
				}{err: err}
				return
			}
			results <- struct {
				result IngestResult
				err    error
			}{result: result}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var skipped int
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("cross-writer idempotent ingest: %v", outcome.err)
		}
		if outcome.result.Version != 1 || outcome.result.Failed != 0 || outcome.result.Applied != 1 {
			t.Fatalf("cross-writer result = %#v", outcome.result)
		}
		if outcome.result.Skipped {
			skipped++
		}
	}
	if skipped != 1 {
		t.Fatalf("cross-writer idempotent replay count = %d, want exactly one replay", skipped)
	}
	head, exists, err := firstCoordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 {
		t.Fatalf("head after idempotent race = %#v exists=%v err=%v", head, exists, err)
	}
}

func TestPostgresCoordinatedIngestCrossWriterRejectsBatchIDReuseWithDifferentIdempotency(t *testing.T) {
	ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-identity")
	secondCoordinator, err := NewPostgresCoordinator(ctx, postgresTestDSN(t), firstCoordinator.schema, firstCoordinator.namespace)
	if err != nil {
		t.Fatalf("new second coordinator: %v", err)
	}
	t.Cleanup(secondCoordinator.Close)
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	first.InstanceID = "writer-a"
	first.CoordinatorRetryLimit = 32
	first.SetCoordinator(firstCoordinator)
	second := NewTenantStore(objects, "test")
	second.InstanceID = "writer-b"
	second.CoordinatorRetryLimit = 32
	second.SetCoordinator(secondCoordinator)
	if _, err := first.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}
	requests := []IngestRequest{
		{
			Source: "agent", CollectorID: "collector-shared", BatchID: "batch-shared", IdempotencyKey: "idempotency-a",
			Items: []IngestItem{{ExternalID: "host:a", Entity: &graph.Entity{ID: "host:a", Kind: "host"}}},
		},
		{
			Source: "agent", CollectorID: "collector-shared", BatchID: "batch-shared", IdempotencyKey: "idempotency-b",
			Items: []IngestItem{{ExternalID: "host:b", Entity: &graph.Entity{ID: "host:b", Kind: "host"}}},
		},
	}
	stores := []*TenantStore{first, second}
	start := make(chan struct{})
	results := make(chan struct {
		result IngestResult
		err    error
	}, len(stores))
	var wait sync.WaitGroup
	for index, store := range stores {
		index, store := index, store
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := ingestDurableWithExplicitRetry(
				ctx, store, "tenant-a", IngestBatchEntry{Request: requests[index]},
			)
			results <- struct {
				result IngestResult
				err    error
			}{result: result, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var applied, failed int
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("cross-writer batch identity ingest: %v", outcome.err)
		}
		if outcome.result.Failed > 0 {
			if outcome.result.ErrorCode != IngestErrorIdempotencyConflict {
				t.Fatalf("failed batch identity result = %#v, want idempotency conflict", outcome.result)
			}
			failed++
			continue
		}
		if outcome.result.Applied != 1 || outcome.result.Version != 1 {
			t.Fatalf("applied batch identity result = %#v", outcome.result)
		}
		applied++
	}
	if applied != 1 || failed != 1 {
		t.Fatalf("batch identity outcomes applied=%d failed=%d, want one of each", applied, failed)
	}
	head, exists, err := firstCoordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 {
		t.Fatalf("head after batch identity race = %#v exists=%v err=%v", head, exists, err)
	}
	loaded, _, err := first.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after batch identity race: %v", err)
	}
	_, hasA := loaded.GetEntity("host:a")
	_, hasB := loaded.GetEntity("host:b")
	if hasA == hasB {
		t.Fatalf("graph batch identity race has host:a=%v host:b=%v, want exactly one", hasA, hasB)
	}
}

func TestPostgresCoordinatedIngestReactivatesLifecycleFailure(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-lifecycle-retry")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "lifecycle-writer"
	store.CoordinatorRetryLimit = 32
	store.SetCoordinator(coordinator)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	config := testIngestServiceConfig(t)
	config.OwnerID = store.InstanceID
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open ingest service: %v", err)
	}
	defer closeIngestService(t, service)
	flushTenant := func() {
		flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
			t.Fatalf("flush tenant: %v", err)
		}
	}

	request := ingestEntityRequest("batch-lifecycle-retry", "host:lifecycle")
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable coordinated tenant: %v", err)
	}
	failed, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept request while tenant disabled: %v", err)
	}
	flushTenant()
	failedResult, err := service.Wait(ctx, failed)
	if err != nil {
		t.Fatalf("wait lifecycle failure: %v", err)
	}
	if failedResult.Failed != 1 || failedResult.Applied != 0 || failedResult.Version != 0 {
		t.Fatalf("lifecycle failure result = %#v, want terminal failed result before CAS", failedResult)
	}
	failedRecord, err := store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("load persisted lifecycle failure: %v", err)
	}
	if failedRecord.Result.Failed != 1 || failedRecord.Result.Applied != 0 {
		t.Fatalf("persisted lifecycle failure = %#v", failedRecord.Result)
	}

	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
		t.Fatalf("reactivate coordinated tenant: %v", err)
	}
	retry, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept request after reactivation: %v", err)
	}
	flushTenant()
	retryResult, err := service.Wait(ctx, retry)
	if err != nil {
		t.Fatalf("wait request after reactivation: %v", err)
	}
	if retryResult.Version != 1 || retryResult.Applied != 1 || retryResult.Failed != 0 {
		t.Fatalf("reactivated request result = %#v, want successful CAS commit at version 1", retryResult)
	}
	committedRecord, err := store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("load replaced lifecycle metadata: %v", err)
	}
	if committedRecord.Result.Version != 1 || committedRecord.Result.Applied != 1 || committedRecord.Result.Failed != 0 {
		t.Fatalf("replaced lifecycle metadata = %#v, want committed result", committedRecord.Result)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 || head.Status != TenantStatusActive {
		t.Fatalf("head after lifecycle retry = %#v exists=%v err=%v", head, exists, err)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after lifecycle retry: %v", err)
	}
	if _, ok := loaded.GetEntity("host:lifecycle"); !ok {
		t.Fatal("reactivated lifecycle request did not publish its entity")
	}
}

func TestPostgresCoordinatedIngestWALLifecycleGenerationFence(t *testing.T) {
	ctx, writerCoordinator := newPostgresIntegrationCoordinator(
		t, "ingest-wal-lifecycle-generation",
	)
	lifecycleCoordinator, err := NewPostgresCoordinator(
		ctx,
		postgresTestDSN(t),
		writerCoordinator.schema,
		writerCoordinator.namespace,
	)
	if err != nil {
		t.Fatalf("new lifecycle coordinator: %v", err)
	}
	t.Cleanup(lifecycleCoordinator.Close)

	objects := NewMemoryStore()
	writerStore := NewTenantStore(objects, "test")
	writerStore.InstanceID = "writer-service"
	writerStore.CoordinatorRetryLimit = 32
	writerStore.SetCoordinator(writerCoordinator)
	lifecycleStore := NewTenantStore(objects, "test")
	lifecycleStore.InstanceID = "lifecycle-writer"
	lifecycleStore.CoordinatorRetryLimit = 32
	lifecycleStore.SetCoordinator(lifecycleCoordinator)
	if _, err := writerStore.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	config := testIngestServiceConfig(t)
	config.OwnerID = writerStore.InstanceID
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	service, err := OpenIngestService(writerStore, config)
	if err != nil {
		t.Fatalf("open writer service: %v", err)
	}
	defer closeIngestService(t, service)

	before, exists, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head before accept exists=%v err=%v", exists, err)
	}
	request := ingestEntityRequest("batch-wal-generation-fence", "host:old-generation")
	accepted, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept pending WAL request: %v", err)
	}
	acceptedHead, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after accept: %v", err)
	}
	if accepted.pending == nil || accepted.pending.envelope.AcceptedGeneration != before.Generation {
		t.Fatalf("accepted WAL generation = %d, want %d", accepted.pending.envelope.AcceptedGeneration, before.Generation)
	}
	if acceptedHead != before {
		t.Fatalf("head changed before lifecycle transition: before=%#v accepted=%#v", before, acceptedHead)
	}

	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	disabled, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after disable: %v", err)
	}
	if disabled.Generation != before.Generation+1 || disabled.Status != TenantStatusDisabled ||
		disabled.GraphVersion != before.GraphVersion {
		t.Fatalf("disabled head = %#v, want generation %d and graph version %d", disabled, before.Generation+1, before.GraphVersion)
	}
	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
		t.Fatalf("reactivate tenant: %v", err)
	}
	active, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after reactivation: %v", err)
	}
	if active.Generation != before.Generation+2 || active.Status != TenantStatusActive ||
		active.GraphVersion != before.GraphVersion {
		t.Fatalf("active head = %#v, want generation %d and graph version %d", active, before.Generation+2, before.GraphVersion)
	}

	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		cancel()
		t.Fatalf("flush old-generation WAL request: %v", err)
	}
	cancel()
	result, err := service.Wait(ctx, accepted)
	if err != nil {
		t.Fatalf("wait old-generation WAL request: %v", err)
	}
	if result.Version != 0 || result.Applied != 0 || result.Failed != 1 {
		t.Fatalf("old-generation WAL result = %#v, want terminal failed result", result)
	}
	status, err := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("status after generation fence: %v", err)
	}
	if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed != 1 ||
		status.Result.Applied != 0 {
		t.Fatalf("status after generation fence = %#v, want failed terminal state", status)
	}

	after, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after old-generation WAL flush: %v", err)
	}
	if after.Generation != active.Generation || after.GraphVersion != before.GraphVersion {
		t.Fatalf("head after fenced WAL = %#v, want generation %d and graph version %d", after, active.Generation, before.GraphVersion)
	}
	loaded, manifest, err := writerStore.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after generation fence: %v", err)
	}
	if manifest.Version != before.GraphVersion {
		t.Fatalf("manifest after fenced WAL version = %d, want %d", manifest.Version, before.GraphVersion)
	}
	if _, ok := loaded.GetEntity("host:old-generation"); ok {
		t.Fatal("old-generation WAL request published an entity after lifecycle transition")
	}
}

func TestPostgresCoordinatedIngestLegacyWALGenerationBoundary(t *testing.T) {
	ctx, writerCoordinator := newPostgresIntegrationCoordinator(
		t, "ingest-wal-legacy-generation",
	)
	lifecycleCoordinator, err := NewPostgresCoordinator(
		ctx,
		postgresTestDSN(t),
		writerCoordinator.schema,
		writerCoordinator.namespace,
	)
	if err != nil {
		t.Fatalf("new lifecycle coordinator: %v", err)
	}
	t.Cleanup(lifecycleCoordinator.Close)

	objects := NewMemoryStore()
	writerStore := NewTenantStore(objects, "test")
	writerStore.InstanceID = "writer-service"
	writerStore.CoordinatorRetryLimit = 32
	writerStore.SetCoordinator(writerCoordinator)
	lifecycleStore := NewTenantStore(objects, "test")
	lifecycleStore.InstanceID = "lifecycle-writer"
	lifecycleStore.CoordinatorRetryLimit = 32
	lifecycleStore.SetCoordinator(lifecycleCoordinator)
	if _, err := writerStore.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	t.Run("generation-one-recovers", func(t *testing.T) {
		config := testIngestServiceConfig(t)
		config.OwnerID = writerStore.InstanceID
		config.FlushInterval = time.Hour
		config.FlushMaxRequests = 1
		request := writeLegacyAcceptedWAL(
			t, config, "tenant-a",
			ingestEntityRequest("batch-legacy-pg-generation-one", "host:legacy-pg-one"),
		)
		service, err := OpenIngestService(writerStore, config)
		if err != nil {
			t.Fatalf("open service from generation-one legacy WAL: %v", err)
		}
		defer closeIngestService(t, service)
		accepted, err := service.Accept(ctx, "tenant-a", request)
		if err != nil {
			t.Fatalf("accept recovered generation-one legacy WAL: %v", err)
		}
		if got := service.pendingAcceptedGeneration(accepted.pending); got != legacyUnboundIngestGeneration {
			t.Fatalf("legacy generation-one pending generation = %d, want %d", got, legacyUnboundIngestGeneration)
		}
		flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
			t.Fatalf("flush generation-one legacy WAL: %v", err)
		}
		result, err := service.Wait(flushCtx, accepted)
		if err != nil {
			t.Fatalf("wait generation-one legacy WAL: %v", err)
		}
		if result.Version != 1 || result.Applied != 1 || result.Failed != 0 {
			t.Fatalf("generation-one legacy WAL result = %#v, want successful version 1", result)
		}
		loaded, manifest, err := writerStore.Load(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("load generation-one graph: %v", err)
		}
		if manifest.Version != 1 {
			t.Fatalf("generation-one manifest version = %d, want 1", manifest.Version)
		}
		if _, ok := loaded.GetEntity("host:legacy-pg-one"); !ok {
			t.Fatal("generation-one legacy WAL entity is not visible")
		}
	})

	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant before legacy generation fence: %v", err)
	}
	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
		t.Fatalf("reactivate tenant before legacy generation fence: %v", err)
	}
	active, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after lifecycle transition: %v", err)
	}
	if active.Generation != 3 || active.Status != TenantStatusActive || active.GraphVersion != 1 {
		t.Fatalf("head after lifecycle transition = %#v, want generation 3 active graph version 1", active)
	}

	t.Run("generation-three-fences", func(t *testing.T) {
		config := testIngestServiceConfig(t)
		config.OwnerID = writerStore.InstanceID
		config.FlushInterval = time.Hour
		config.FlushMaxRequests = 1
		request := writeLegacyAcceptedWAL(
			t, config, "tenant-a",
			ingestEntityRequest("batch-legacy-pg-generation-three", "host:legacy-pg-three"),
		)
		service, err := OpenIngestService(writerStore, config)
		if err != nil {
			t.Fatalf("open service from generation-three legacy WAL: %v", err)
		}
		defer closeIngestService(t, service)
		accepted, err := service.Accept(ctx, "tenant-a", request)
		if err != nil {
			t.Fatalf("accept recovered generation-three legacy WAL: %v", err)
		}
		if got := service.pendingAcceptedGeneration(accepted.pending); got != legacyUnboundIngestGeneration {
			t.Fatalf("legacy generation-three pending generation = %d, want %d", got, legacyUnboundIngestGeneration)
		}
		flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
			t.Fatalf("flush generation-three legacy WAL: %v", err)
		}
		result, err := service.Wait(flushCtx, accepted)
		if err != nil {
			t.Fatalf("wait generation-three legacy WAL: %v", err)
		}
		if result.Version != 0 || result.Applied != 0 || result.Failed != 1 {
			t.Fatalf("generation-three legacy WAL result = %#v, want lifecycle-fenced failure", result)
		}
		status, err := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
		if err != nil {
			t.Fatalf("status after generation-three legacy WAL fence: %v", err)
		}
		if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed != 1 || status.Result.Applied != 0 {
			t.Fatalf("status after generation-three legacy WAL fence = %#v, want failed terminal state", status)
		}
		after, _, err := writerCoordinator.Head(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("head after generation-three legacy WAL fence: %v", err)
		}
		if after.Generation != active.Generation || after.GraphVersion != active.GraphVersion {
			t.Fatalf("head after generation-three legacy WAL fence = %#v, want generation %d graph version %d", after, active.Generation, active.GraphVersion)
		}
		loaded, manifest, err := writerStore.Load(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("load generation-three graph: %v", err)
		}
		if manifest.Version != active.GraphVersion {
			t.Fatalf("generation-three manifest version = %d, want %d", manifest.Version, active.GraphVersion)
		}
		if _, ok := loaded.GetEntity("host:legacy-pg-three"); ok {
			t.Fatal("generation-three legacy WAL entity was published after lifecycle transition")
		}
	})
}

func postgresTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	return dsn
}

type countingPostgresIngestCoordinator struct {
	*PostgresCoordinator
	mu           sync.Mutex
	publishCalls int
}

func (c *countingPostgresIngestCoordinator) PublishIngestBatch(
	ctx context.Context,
	request IngestBatchPublishRequest,
) (CoordinationHead, bool, error) {
	c.mu.Lock()
	c.publishCalls++
	c.mu.Unlock()
	return c.PostgresCoordinator.PublishIngestBatch(ctx, request)
}

func (c *countingPostgresIngestCoordinator) publishBatchCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publishCalls
}

type ingestCASCASRaceGate struct {
	mu             sync.Mutex
	acquireCalls   int
	publishCalls   int
	acquireReady   chan struct{}
	publishReady   chan struct{}
	acquireRelease sync.Once
	publishRelease sync.Once
}

func newIngestCASCASRaceGate() *ingestCASCASRaceGate {
	return &ingestCASCASRaceGate{
		acquireReady: make(chan struct{}),
		publishReady: make(chan struct{}),
	}
}

func (g *ingestCASCASRaceGate) recordAcquire() {
	g.mu.Lock()
	g.acquireCalls++
	if g.acquireCalls == 2 {
		g.acquireRelease.Do(func() { close(g.acquireReady) })
	}
	g.mu.Unlock()
}

func (g *ingestCASCASRaceGate) recordPublish() {
	g.mu.Lock()
	g.publishCalls++
	if g.publishCalls == 2 {
		g.publishRelease.Do(func() { close(g.publishReady) })
	}
	g.mu.Unlock()
}

func (g *ingestCASCASRaceGate) waitAcquire(ctx context.Context) error {
	select {
	case <-g.acquireReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *ingestCASCASRaceGate) waitPublish(ctx context.Context) error {
	select {
	case <-g.publishReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *ingestCASCASRaceGate) publishCallCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.publishCalls
}

type ingestCASCASRaceCoordinator struct {
	*PostgresCoordinator
	gate *ingestCASCASRaceGate
}

func (c *ingestCASCASRaceCoordinator) AcquireIngestPublishSlot(
	ctx context.Context,
	tenantID string,
	owner string,
	ttl time.Duration,
) (CoordinatorTaskLease, CoordinationHead, bool, bool, error) {
	head, exists, err := c.PostgresCoordinator.Head(ctx, tenantID)
	if err != nil {
		return CoordinatorTaskLease{}, CoordinationHead{}, false, false, err
	}
	c.gate.recordAcquire()
	if err := c.gate.waitAcquire(ctx); err != nil {
		return CoordinatorTaskLease{}, CoordinationHead{}, false, false, err
	}
	return CoordinatorTaskLease{
		TenantID:   tenantID,
		TaskType:   coordinatorIngestPublishTaskType,
		OwnerToken: owner,
		FenceEpoch: 1,
		ExpiresAt:  time.Now().Add(ttl),
	}, head, exists, true, nil
}

func (c *ingestCASCASRaceCoordinator) PublishIngestBatch(
	ctx context.Context,
	request IngestBatchPublishRequest,
) (CoordinationHead, bool, error) {
	c.gate.recordPublish()
	if err := c.gate.waitPublish(ctx); err != nil {
		return CoordinationHead{}, false, err
	}
	// The test gate deliberately bypasses the optional publish lease so both
	// candidates reach the real PostgreSQL head CAS concurrently.
	request.PublishLease = nil
	return c.PostgresCoordinator.PublishIngestBatch(ctx, request)
}

func ingestCohortWithExplicitRetry(
	ctx context.Context,
	store *TenantStore,
	tenantID string,
	entries []IngestBatchEntry,
) ([]IngestResult, error) {
	var lastErr error
	for attempt := 0; attempt < 64; attempt++ {
		results, err := store.IngestDurableBatch(ctx, tenantID, entries)
		if err == nil {
			return results, nil
		}
		if !errors.Is(err, ErrConflict) &&
			!errors.Is(err, ErrWriteConflict) &&
			!errors.Is(err, ErrIdempotencyInProgress) &&
			!errors.Is(err, ErrTaskLeaseHeld) &&
			!errors.Is(err, ErrCoordinatorUnavailable) {
			return nil, err
		}
		lastErr = err
		timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("coordinated ingest cohort retry budget exhausted: %w", lastErr)
}

// IngestDurableBatch is one coordinated attempt; the service scheduler owns
// retry/rebase. These direct-store integration cases model that boundary with
// an explicit retry loop so a single expected CAS race is not treated as a
// terminal ingest failure.
func ingestDurableWithExplicitRetry(
	ctx context.Context,
	store *TenantStore,
	tenantID string,
	entry IngestBatchEntry,
) (IngestResult, error) {
	var lastErr error
	for attempt := 0; attempt < 64; attempt++ {
		results, err := store.IngestDurableBatch(ctx, tenantID, []IngestBatchEntry{entry})
		if err == nil {
			if len(results) != 1 {
				return IngestResult{}, fmt.Errorf("coordinated ingest returned %d results", len(results))
			}
			return results[0], nil
		}
		if !errors.Is(err, ErrConflict) &&
			!errors.Is(err, ErrWriteConflict) &&
			!errors.Is(err, ErrIdempotencyInProgress) &&
			!errors.Is(err, ErrTaskLeaseHeld) &&
			!errors.Is(err, ErrCoordinatorUnavailable) {
			return IngestResult{}, err
		}
		lastErr = err
		backoff := time.Duration(attempt+1) * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return IngestResult{}, ctx.Err()
		}
	}
	return IngestResult{}, fmt.Errorf("coordinated ingest retry budget exhausted: %w", lastErr)
}

func TestPostgresCoordinatedDirectIngestRejectsBatchIDReuseWithDifferentIdempotency(t *testing.T) {
	ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, "direct-ingest-batch-identity")
	secondCoordinator, err := NewPostgresCoordinator(ctx, postgresTestDSN(t), firstCoordinator.schema, firstCoordinator.namespace)
	if err != nil {
		t.Fatalf("new second coordinator: %v", err)
	}
	t.Cleanup(secondCoordinator.Close)
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	first.InstanceID = "writer-a"
	first.CoordinatorRetryLimit = 32
	first.SetCoordinator(firstCoordinator)
	second := NewTenantStore(objects, "test")
	second.InstanceID = "writer-b"
	second.CoordinatorRetryLimit = 32
	second.SetCoordinator(secondCoordinator)
	if _, err := first.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}
	firstRequest := IngestRequest{
		Source: "agent", CollectorID: "collector-shared", BatchID: "batch-shared", IdempotencyKey: "idempotency-a",
		Items: []IngestItem{{ExternalID: "host:a", Entity: &graph.Entity{ID: "host:a", Kind: "host"}}},
	}
	firstResult, err := first.Ingest(ctx, "tenant-a", firstRequest)
	if err != nil || firstResult.Applied != 1 || firstResult.Version != 1 {
		t.Fatalf("first direct ingest result = %#v err=%v", firstResult, err)
	}
	secondRequest := IngestRequest{
		Source: "agent", CollectorID: "collector-shared", BatchID: "batch-shared", IdempotencyKey: "idempotency-b",
		Items: []IngestItem{{ExternalID: "host:b", Entity: &graph.Entity{ID: "host:b", Kind: "host"}}},
	}
	secondResult, err := second.Ingest(ctx, "tenant-a", secondRequest)
	if err != nil {
		t.Fatalf("second direct ingest: %v", err)
	}
	if secondResult.Failed == 0 || secondResult.ErrorCode != IngestErrorIdempotencyConflict {
		t.Fatalf("second direct ingest result = %#v, want idempotency conflict", secondResult)
	}
	head, exists, err := firstCoordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 {
		t.Fatalf("head after direct batch identity conflict = %#v exists=%v err=%v", head, exists, err)
	}
	loaded, _, err := first.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after direct batch identity conflict: %v", err)
	}
	if _, exists := loaded.GetEntity("host:b"); exists {
		t.Fatal("conflicting direct ingest committed host:b")
	}
}
