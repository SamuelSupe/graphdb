package storage

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type csvImportReader struct {
	reader  *csv.Reader
	headers []string
	row     int
}

func newCSVImportReader(data []byte) (*csvImportReader, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.TrimLeadingSpace = true
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	seen := map[string]struct{}{}
	for i, header := range headers {
		header = strings.TrimSpace(header)
		if header == "" {
			return nil, fmt.Errorf("CSV header %d is empty", i+1)
		}
		key := strings.ToLower(header)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("CSV header %q is duplicated", header)
		}
		seen[key] = struct{}{}
		headers[i] = header
	}
	if _, ok := seen["record_type"]; !ok {
		return nil, fmt.Errorf("CSV requires a record_type header")
	}
	reader.FieldsPerRecord = len(headers)
	return &csvImportReader{reader: reader, headers: headers, row: 1}, nil
}

func (r *csvImportReader) Next() (IngestItem, int, bool, error) {
	record, err := r.reader.Read()
	if err == io.EOF {
		return IngestItem{}, 0, false, nil
	}
	r.row++
	if err != nil {
		return IngestItem{}, r.row, true, fmt.Errorf("read CSV row %d: %w", r.row, err)
	}
	item, err := csvRecordToIngestItem(r.headers, record)
	if err != nil {
		return IngestItem{}, r.row, true, fmt.Errorf("decode CSV row %d: %w", r.row, err)
	}
	return item, r.row, true, nil
}

func csvRecordToIngestItem(headers []string, record []string) (IngestItem, error) {
	cells := make(map[string]string, len(headers))
	for i, header := range headers {
		cells[strings.ToLower(header)] = strings.TrimSpace(record[i])
	}
	recordType := strings.ToLower(cells["record_type"])
	item := IngestItem{ExternalID: cells["external_id"]}
	switch recordType {
	case "entity", "node":
		entity := graph.Entity{
			ID:         cells["id"],
			Kind:       firstValue(cells["entity_type"], cells["kind"]),
			Source:     cells["source"],
			ExternalID: cells["external_id"],
		}
		fields, err := csvFields(headers, cells)
		if err != nil {
			return IngestItem{}, err
		}
		entity.Fields = fields
		if labels := cells["labels"]; labels != "" {
			parsed, err := parseCSVLabels(labels)
			if err != nil {
				return IngestItem{}, err
			}
			if err := graph.SetEntityLabels(&entity, parsed); err != nil {
				return IngestItem{}, err
			}
		}
		item.Entity = &entity
	case "edge", "relationship":
		fields, err := csvFields(headers, cells)
		if err != nil {
			return IngestItem{}, err
		}
		item.Edge = &graph.Edge{
			ID: cells["id"], Type: firstValue(cells["relation_type"], cells["type"]),
			From: cells["from"], To: cells["to"], Source: cells["source"],
			ExternalID: cells["external_id"], Fields: fields,
		}
	case "delete_entity", "delete_node":
		item.DeleteEntity = &graph.EntityDeleteRequest{
			ID: cells["id"], Kind: firstValue(cells["entity_type"], cells["kind"]),
			Source: cells["source"], ExternalID: cells["external_id"], Reason: cells["reason"],
		}
	case "delete_edge", "delete_relationship":
		item.DeleteEdge = &graph.EdgeDeleteRequest{
			ID: cells["id"], Type: firstValue(cells["relation_type"], cells["type"]),
			From: cells["from"], To: cells["to"], Source: cells["source"], Reason: cells["reason"],
		}
	case "entity_type":
		var value graph.EntityType
		if err := decodeCSVPayload(cells, &value); err != nil {
			return IngestItem{}, err
		}
		item.CIType = &value
	case "relation_type":
		var value graph.RelationType
		if err := decodeCSVPayload(cells, &value); err != nil {
			return IngestItem{}, err
		}
		item.Relation = &value
	default:
		return IngestItem{}, fmt.Errorf("unsupported record_type %q", cells["record_type"])
	}
	return item, nil
}

func decodeCSVPayload(cells map[string]string, value any) error {
	payload := firstValue(cells["payload"], cells["definition"])
	if payload == "" {
		return fmt.Errorf("record requires a JSON payload column")
	}
	if err := json.Unmarshal([]byte(payload), value); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	return nil
}

func csvFields(headers []string, cells map[string]string) (graph.Fields, error) {
	fields := graph.Fields{}
	if raw := firstValue(cells["fields_json"], cells["fields"]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			return nil, fmt.Errorf("fields must be a JSON object: %w", err)
		}
	}
	for _, header := range headers {
		key := strings.ToLower(header)
		if csvReservedColumn(key) || cells[key] == "" {
			continue
		}
		fields[header] = parseCSVScalar(cells[key])
	}
	if len(fields) == 0 {
		return nil, nil
	}
	return fields, nil
}

func csvReservedColumn(column string) bool {
	switch column {
	case "record_type", "payload", "definition", "id", "kind", "entity_type", "labels", "type", "relation_type", "from", "to", "source", "external_id", "reason", "fields", "fields_json":
		return true
	default:
		return false
	}
}

func parseCSVScalar(value string) any {
	var parsed any
	if json.Unmarshal([]byte(value), &parsed) == nil {
		return parsed
	}
	return value
}

func parseCSVLabels(value string) ([]string, error) {
	if strings.HasPrefix(strings.TrimSpace(value), "[") {
		var labels []string
		if err := json.Unmarshal([]byte(value), &labels); err != nil {
			return nil, fmt.Errorf("labels must be a JSON string array or pipe-delimited text: %w", err)
		}
		return labels, nil
	}
	parts := strings.Split(value, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts, nil
}
