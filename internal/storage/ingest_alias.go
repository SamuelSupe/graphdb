package storage

import (
	"encoding/json"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (item *IngestItem) UnmarshalJSON(data []byte) error {
	type ingestItemAlias IngestItem
	var wire struct {
		*ingestItemAlias
		EntityType *graph.EntityType `json:"entity_type"`
	}
	wire.ingestItemAlias = (*ingestItemAlias)(item)
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.EntityType == nil {
		return nil
	}
	if item.CIType != nil {
		return fmt.Errorf("entity_type and ci_type cannot be used together")
	}
	item.CIType = wire.EntityType
	return nil
}
