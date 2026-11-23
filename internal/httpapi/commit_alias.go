package httpapi

import (
	"encoding/json"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (request *CommitRequest) UnmarshalJSON(data []byte) error {
	var raw struct {
		ExpectedVersion *int64 `json:"expected_version,omitempty"`
		IdempotencyKey  string `json:"idempotency_key,omitempty"`
		Mutations       struct {
			graph.Mutations
			UpsertEntityTypes []graph.EntityType `json:"upsert_entity_types"`
			DeleteEntityTypes []string           `json:"delete_entity_types"`
		} `json:"mutations"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	request.ExpectedVersion = raw.ExpectedVersion
	request.IdempotencyKey = raw.IdempotencyKey
	request.Mutations = raw.Mutations.Mutations
	if len(raw.Mutations.UpsertEntityTypes) > 0 {
		if len(request.Mutations.UpsertCITypes) > 0 {
			return fmt.Errorf("mutations.upsert_entity_types and mutations.upsert_ci_types cannot be used together")
		}
		request.Mutations.UpsertCITypes = raw.Mutations.UpsertEntityTypes
	}
	if len(raw.Mutations.DeleteEntityTypes) > 0 {
		if len(request.Mutations.DeleteCITypes) > 0 {
			return fmt.Errorf("mutations.delete_entity_types and mutations.delete_ci_types cannot be used together")
		}
		request.Mutations.DeleteCITypes = raw.Mutations.DeleteEntityTypes
	}
	return nil
}
