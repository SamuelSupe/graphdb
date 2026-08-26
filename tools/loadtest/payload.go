package main

import (
	"fmt"
	"strconv"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func schemaMutations() graph.Mutations {
	return graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{
				Name: "host",
				Fields: map[string]graph.FieldSpec{
					"hostname": {Type: "string", Required: true, Unique: true, Indexed: true},
					"region":   {Type: "string", Indexed: true},
				},
			},
			{Name: "service", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Required: true, Unique: true, Indexed: true}}},
		},
		UpsertEntities: []graph.Entity{
			{ID: "host:seed", Kind: "host", Fields: graph.Fields{"hostname": "seed-host", "region": "region-0"}},
			{ID: "service:seed", Kind: "service", Fields: graph.Fields{"name": "seed-service"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:seed-runs-on", Type: "runs_on", From: "service:seed", To: "host:seed"}},
	}
}

func batchRequest(batch int, batchSize int, collector string, workingSet int) storage.IngestRequest {
	items := make([]storage.IngestItem, 0, batchSize*3)
	for i := 0; i < batchSize; i++ {
		n := batch*batchSize + i
		if workingSet > 0 {
			n %= workingSet
		}
		hostID := fmt.Sprintf("host:%06d", n)
		serviceID := fmt.Sprintf("service:%06d", n)
		items = append(items,
			storage.IngestItem{ExternalID: hostID, Entity: &graph.Entity{
				ID: hostID, Kind: "host", Confidence: 0.9, SourceRank: 10,
				Fields: graph.Fields{"hostname": fmt.Sprintf("app-%06d", n), "region": regionName(n % 8), "generation": batch},
			}},
			storage.IngestItem{ExternalID: serviceID, Entity: &graph.Entity{
				ID: serviceID, Kind: "service", Confidence: 0.9, SourceRank: 10,
				Fields: graph.Fields{"name": fmt.Sprintf("svc-%06d", n), "generation": batch},
			}},
			storage.IngestItem{ExternalID: "edge-" + serviceID, Edge: &graph.Edge{
				ID: fmt.Sprintf("edge:%06d", n), Type: "runs_on", From: serviceID, To: hostID, Fields: graph.Fields{"generation": batch},
			}},
		)
	}
	return storage.IngestRequest{
		Source:         "loadtest",
		CollectorID:    collector,
		BatchID:        fmt.Sprintf("batch-%06d", batch),
		IdempotencyKey: fmt.Sprintf("batch-%06d", batch),
		Cursor:         fmt.Sprintf("cursor-%06d", batch),
		Items:          items,
	}
}

func batchRequestJSON(batch int, batchSize int, collector string, workingSet int) encodedJSON {
	return appendBatchRequestJSON(make([]byte, 0, batchRequestJSONCapacity(batchSize)), batch, batchSize, collector, workingSet)
}

func appendBatchRequestJSON(data []byte, batch int, batchSize int, collector string, workingSet int) encodedJSON {
	data = append(data, `{"source":"loadtest","collector_id":`...)
	data = strconv.AppendQuote(data, collector)
	data = append(data, `,"batch_id":"batch-`...)
	data = appendPaddedDecimal(data, batch, 6)
	data = append(data, `","idempotency_key":"batch-`...)
	data = appendPaddedDecimal(data, batch, 6)
	data = append(data, `","cursor":"cursor-`...)
	data = appendPaddedDecimal(data, batch, 6)
	data = append(data, `","items":[`...)
	for index := range batchSize {
		if index > 0 {
			data = append(data, ',')
		}
		n := batch*batchSize + index
		if workingSet > 0 {
			n %= workingSet
		}
		data = appendHostItemJSON(data, batch, n)
		data = append(data, ',')
		data = appendServiceItemJSON(data, batch, n)
		data = append(data, ',')
		data = appendEdgeItemJSON(data, batch, n)
	}
	return append(data, ']', '}')
}

func batchRequestJSONCapacity(batchSize int) int {
	return batchSize*1600 + 256
}

func appendHostItemJSON(data []byte, batch int, n int) []byte {
	data = append(data, `{"external_id":"host:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","entity":{"id":"host:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","kind":"host","fields":{"generation":`...)
	data = strconv.AppendInt(data, int64(batch), 10)
	data = append(data, `,"hostname":"app-`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","region":"region-`...)
	data = strconv.AppendInt(data, int64(n%8), 10)
	return append(data, `"},"confidence":0.9,"source_priority":10,"version":0,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z"}}`...)
}

func appendServiceItemJSON(data []byte, batch int, n int) []byte {
	data = append(data, `{"external_id":"service:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","entity":{"id":"service:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","kind":"service","fields":{"generation":`...)
	data = strconv.AppendInt(data, int64(batch), 10)
	data = append(data, `,"name":"svc-`...)
	data = appendPaddedDecimal(data, n, 6)
	return append(data, `"},"confidence":0.9,"source_priority":10,"version":0,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z"}}`...)
}

func appendEdgeItemJSON(data []byte, batch int, n int) []byte {
	data = append(data, `{"external_id":"edge-service:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","edge":{"id":"edge:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","type":"runs_on","from":"service:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","to":"host:`...)
	data = appendPaddedDecimal(data, n, 6)
	data = append(data, `","fields":{"generation":`...)
	data = strconv.AppendInt(data, int64(batch), 10)
	return append(data, `},"version":0,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z"}}`...)
}

func appendPaddedDecimal(data []byte, value int, width int) []byte {
	var buffer [20]byte
	digits := strconv.AppendInt(buffer[:0], int64(value), 10)
	for padding := width - len(digits); padding > 0; padding-- {
		data = append(data, '0')
	}
	return append(data, digits...)
}

func collectorName(index int) string {
	return fmt.Sprintf("collector-%02d", index)
}

func matchQuery(region string) query.Request {
	return query.Request{
		Op:       "profile",
		TargetOp: "match",
		Kind:     "host",
		Where:    []query.Filter{{Field: "region", Op: "eq", Value: region}},
		Sort:     []query.SortSpec{{Field: "hostname"}},
		Project:  []string{"id", "hostname", "region"},
		Limit:    20,
	}
}

func traverseQuery(batch int) query.Request {
	return query.Request{Op: "traverse", ID: "service:seed", Direction: "out", RelationType: "runs_on", Depth: 1, Limit: 5}
}

func regionName(value int) string {
	return fmt.Sprintf("region-%d", value)
}
