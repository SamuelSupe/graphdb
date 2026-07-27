package httpapi

import (
	"encoding/json"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (request *CommitRequest) UnmarshalJSON(data []byte) error {
	type commitRequestAlias CommitRequest
	if err := json.Unmarshal(data, (*commitRequestAlias)(request)); err != nil {
		return err
	}

	var envelope struct {
		Mutations json.RawMessage `json:"mutations"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Mutations) == 0 {
		return err
	}
	var aliases struct {
		UpsertEntityTypes []graph.EntityType `json:"upsert_entity_types"`
		DeleteEntityTypes []string           `json:"delete_entity_types"`
	}
	if err := json.Unmarshal(envelope.Mutations, &aliases); err != nil {
		return err
	}
	if len(aliases.UpsertEntityTypes) > 0 {
		if len(request.Mutations.UpsertCITypes) > 0 {
			return fmt.Errorf("mutations.upsert_entity_types and mutations.upsert_ci_types cannot be used together")
		}
		request.Mutations.UpsertCITypes = aliases.UpsertEntityTypes
	}
	if len(aliases.DeleteEntityTypes) > 0 {
		if len(request.Mutations.DeleteCITypes) > 0 {
			return fmt.Errorf("mutations.delete_entity_types and mutations.delete_ci_types cannot be used together")
		}
		request.Mutations.DeleteCITypes = aliases.DeleteEntityTypes
	}
	return nil
}
