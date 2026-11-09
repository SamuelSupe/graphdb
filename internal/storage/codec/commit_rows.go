package codec

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
)

func CommitRows(commit graph.Commit) ([]ParquetCommitRow, error) {
	rows := []ParquetCommitRow{{Kind: CommitRowMetadata}}
	if commit.IdentityEncoding != 0 {
		rows = append(rows, ParquetCommitRow{Kind: "identity_encoding", Name: fmt.Sprint(commit.IdentityEncoding)})
	}
	for i, ciType := range commit.Mutations.UpsertCITypes {
		rows = append(rows, CITypeRows(i, ciType)...)
	}
	for i, name := range commit.Mutations.DeleteCITypes {
		rows = append(rows, ParquetCommitRow{Kind: commitRowDeleteCIType, ParentOrdinal: i, Name: name})
	}
	for i, relationType := range commit.Mutations.UpsertRelationTypes {
		rows = append(rows, RelationTypeRows(i, relationType)...)
	}
	for i, name := range commit.Mutations.DeleteRelationTypes {
		rows = append(rows, ParquetCommitRow{Kind: commitRowDeleteRelationType, ParentOrdinal: i, Name: name})
	}
	for i, entity := range commit.Mutations.UpsertEntities {
		entityRows, err := EntityMutationRows(CommitRowUpsertEntity, i, 0, entity)
		if err != nil {
			return nil, err
		}
		rows = append(rows, entityRows...)
	}
	for i, id := range commit.Mutations.DeleteEntities {
		rows = append(rows, ParquetCommitRow{Kind: CommitRowDeleteEntity, ParentOrdinal: i, ID: id})
	}
	for i, request := range commit.Mutations.DeleteEntityRequests {
		rows = append(rows, ParquetCommitRow{
			Kind:           commitRowDeleteEntityRequest,
			ParentOrdinal:  i,
			ID:             request.ID,
			KindName:       request.Kind,
			Source:         request.Source,
			ExternalID:     request.ExternalID,
			SourcePriority: request.SourceRank,
			Confidence:     request.Confidence,
			Reason:         request.Reason,
		})
	}
	for i, request := range commit.Mutations.MarkSourceStale {
		rows = append(rows, ParquetCommitRow{
			Kind:           commitRowMarkSourceStale,
			ParentOrdinal:  i,
			Source:         request.Source,
			KindName:       request.Kind,
			Action:         request.Action,
			SourcePriority: request.SourceRank,
			Confidence:     request.Confidence,
			Reason:         request.Reason,
		})
		for j, externalID := range request.ObservedExternalIDs {
			rows = append(rows, ParquetCommitRow{Kind: commitRowStaleObservedExternal, ParentOrdinal: i, ChildOrdinal: j, Value: StringCommitValue(externalID)})
		}
	}
	for i, edge := range commit.Mutations.UpsertEdges {
		edgeRows, err := EdgeMutationRows(CommitRowUpsertEdge, i, edge)
		if err != nil {
			return nil, err
		}
		rows = append(rows, edgeRows...)
	}
	for i, id := range commit.Mutations.DeleteEdges {
		rows = append(rows, ParquetCommitRow{Kind: commitRowDeleteEdge, ParentOrdinal: i, ID: id})
	}
	for i, request := range commit.Mutations.DeleteEdgeRequests {
		rows = append(rows, ParquetCommitRow{
			Kind:           commitRowDeleteEdgeRequest,
			ParentOrdinal:  i,
			ID:             request.ID,
			TypeName:       request.Type,
			From:           request.From,
			To:             request.To,
			Source:         request.Source,
			SourcePriority: request.SourceRank,
			Confidence:     request.Confidence,
			Reason:         request.Reason,
		})
	}
	for i, merge := range commit.Mutations.MergeEntities {
		rows = append(rows, ParquetCommitRow{Kind: commitRowMergeEntity, ParentOrdinal: i, TargetID: merge.TargetID})
		for j, sourceID := range merge.SourceIDs {
			rows = append(rows, ParquetCommitRow{Kind: commitRowMergeSource, ParentOrdinal: i, ChildOrdinal: j, SourceID: sourceID})
		}
	}
	for i, split := range commit.Mutations.SplitEntities {
		rows = append(rows, ParquetCommitRow{Kind: commitRowSplitRequest, ParentOrdinal: i, SourceID: split.SourceID})
		for j, entity := range split.Entities {
			entityRows, err := EntityMutationRows(commitRowSplitEntity, i, j, entity)
			if err != nil {
				return nil, err
			}
			rows = append(rows, entityRows...)
		}
	}
	return rows, nil
}

func CITypeRows(ordinal int, ciType graph.CIType) []ParquetCommitRow {
	rows := []ParquetCommitRow{{
		Kind:          CommitRowUpsertCIType,
		ParentOrdinal: ordinal,
		Name:          ciType.Name,
		DisplayName:   ciType.DisplayName,
	}}
	for i, extend := range ciType.Extends {
		rows = append(rows, ParquetCommitRow{Kind: CommitRowCITypeExtends, ParentOrdinal: ordinal, ChildOrdinal: i, Value: StringCommitValue(extend)})
	}
	fieldNames := sortedFieldSpecKeys(ciType.Fields)
	for i, fieldName := range fieldNames {
		spec := ciType.Fields[fieldName]
		rows = append(rows, ParquetCommitRow{
			Kind:          CommitRowCITypeField,
			ParentOrdinal: ordinal,
			ChildOrdinal:  i,
			EntryKey:      fieldName,
			FieldType:     spec.Type,
			Strategy:      spec.MergeStrategy,
			Required:      spec.Required,
			Indexed:       spec.Indexed,
			Unique:        spec.Unique,
		})
		for j, enumValue := range spec.Enum {
			value, err := parquetio.ValueFromAny(enumValue)
			if err == nil {
				rows = append(rows, ParquetCommitRow{Kind: CommitRowCITypeFieldEnum, ParentOrdinal: ordinal, ChildOrdinal: i, NestedOrdinal: j, EntryKey: fieldName, Value: value})
			}
		}
		if spec.Default != nil {
			if value, err := parquetio.ValueFromAny(spec.Default); err == nil {
				rows = append(rows, ParquetCommitRow{Kind: CommitRowCITypeFieldDefault, ParentOrdinal: ordinal, ChildOrdinal: i, EntryKey: fieldName, Value: value})
			}
		}
	}
	for i, identity := range ciType.IdentityKeys {
		rows = append(rows, ParquetCommitRow{
			Kind:          CommitRowCITypeIdentity,
			ParentOrdinal: ordinal,
			ChildOrdinal:  i,
			Name:          identity.Name,
			Confidence:    identity.ConfidenceThreshold,
			Strategy:      identity.Strategy,
		})
		for j, field := range identity.Fields {
			rows = append(rows, ParquetCommitRow{Kind: CommitRowCITypeIdentityField, ParentOrdinal: ordinal, ChildOrdinal: i, NestedOrdinal: j, Value: StringCommitValue(field)})
		}
	}
	return rows
}

func RelationTypeRows(ordinal int, relationType graph.RelationType) []ParquetCommitRow {
	rows := []ParquetCommitRow{{
		Kind:            CommitRowUpsertRelationType,
		ParentOrdinal:   ordinal,
		Name:            relationType.Name,
		DisplayName:     relationType.DisplayName,
		ReverseName:     relationType.ReverseName,
		FromKind:        relationType.FromKind,
		ToKind:          relationType.ToKind,
		Directed:        relationType.Directed,
		Cardinality:     relationType.Cardinality,
		ImpactDirection: relationType.ImpactDirection,
		AllowCrossKind:  relationType.AllowCrossKind,
		Standard:        relationType.Standard,
	}}
	for i, kind := range relationType.FromKinds {
		rows = append(rows, ParquetCommitRow{Kind: CommitRowRelationFromKind, ParentOrdinal: ordinal, ChildOrdinal: i, Value: StringCommitValue(kind)})
	}
	for i, kind := range relationType.ToKinds {
		rows = append(rows, ParquetCommitRow{Kind: CommitRowRelationToKind, ParentOrdinal: ordinal, ChildOrdinal: i, Value: StringCommitValue(kind)})
	}
	return rows
}

func EntityMutationRows(kind string, parent int, child int, entity graph.Entity) ([]ParquetCommitRow, error) {
	components, err := EntityRows(entity)
	if err != nil {
		return nil, err
	}
	rows := make([]ParquetCommitRow, 0, len(components))
	for _, component := range components {
		row := ParquetCommitRow{
			Kind:           kind,
			ComponentKind:  component.Kind,
			ParentOrdinal:  parent,
			ChildOrdinal:   child,
			NestedOrdinal:  component.Ordinal,
			EntryKey:       component.Key,
			Value:          component.Value,
			ID:             entity.ID,
			KindName:       entity.Kind,
			Source:         entity.Source,
			ExternalID:     entity.ExternalID,
			Confidence:     entity.Confidence,
			SourcePriority: entity.SourceRank,
			VersionValue:   entity.Version,
			CreatedAtValue: entity.CreatedAt,
			UpdatedAtValue: entity.UpdatedAt,
			SplitFrom:      entity.SplitFrom,
			Strategy:       entity.FieldWriteModes[component.Key],
			FieldSource:    component.FieldSource,
			EntitySource:   component.EntitySource,
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func EdgeMutationRows(kind string, parent int, edge graph.Edge) ([]ParquetCommitRow, error) {
	components, err := EdgeRows(edge)
	if err != nil {
		return nil, err
	}
	rows := make([]ParquetCommitRow, 0, len(components))
	for _, component := range components {
		row := ParquetCommitRow{
			Kind:           kind,
			ComponentKind:  component.Kind,
			ParentOrdinal:  parent,
			ChildOrdinal:   component.Ordinal,
			EntryKey:       component.Key,
			Value:          component.Value,
			ID:             edge.ID,
			TypeName:       edge.Type,
			From:           edge.From,
			To:             edge.To,
			Source:         edge.Source,
			ExternalID:     edge.ExternalID,
			Confidence:     edge.Confidence,
			SourcePriority: edge.SourceRank,
			VersionValue:   edge.Version,
			CreatedAtValue: edge.CreatedAt,
			UpdatedAtValue: edge.UpdatedAt,
			FieldSource:    component.FieldSource,
			EdgeSource:     component.EdgeSource,
		}
		rows = append(rows, row)
	}
	return rows, nil
}
