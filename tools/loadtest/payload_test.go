package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestBatchRequestJSONMatchesCanonicalEncoding(t *testing.T) {
	for _, test := range []struct {
		batch      int
		batchSize  int
		collector  string
		workingSet int
	}{
		{batch: 0, batchSize: 1, collector: "collector-00", workingSet: 20000},
		{batch: 127, batchSize: 200, collector: "collector-01", workingSet: 20000},
		{batch: 1_000_001, batchSize: 2, collector: `collector-"quoted`, workingSet: 0},
	} {
		want, err := json.Marshal(batchRequest(test.batch, test.batchSize, test.collector, test.workingSet))
		if err != nil {
			t.Fatal(err)
		}
		got := batchRequestJSON(test.batch, test.batchSize, test.collector, test.workingSet)
		if !bytes.Equal(got, want) {
			t.Fatalf("batch %d payload differs at byte %d", test.batch, firstDifferentByte(got, want))
		}
	}
}

func firstDifferentByte(left []byte, right []byte) int {
	limit := min(len(left), len(right))
	for index := range limit {
		if left[index] != right[index] {
			return index
		}
	}
	return limit
}
