package codec

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
)

// CommitBuilder reconstructs shared mutation rows without exposing its maps.
// Its zero value is ready for Apply; discard it after a decoding error.
type CommitBuilder struct {
	key            string
	contentHash    string
	commit         graph.Commit
	ciTypes        map[int]*ciTypeBuild
	relations      map[int]*relationBuild
	entities       map[int]*entityBuild
	splitEntities  map[int]map[int]*entityBuild
	edges          map[int]*edgeBuild
	entityOrdinals *int64
}

func NewCommitBuilder(rows int64) *CommitBuilder {
	return &CommitBuilder{entityOrdinals: &rows}
}

type ciTypeBuild struct {
	item       graph.CIType
	fields     map[int]string
	identities map[int]*identityBuild
}

type identityBuild struct {
	item graph.IdentityKey
}

type relationBuild struct {
	item graph.RelationType
}

type entityBuild struct {
	item        graph.Entity
	initialized bool
}

type edgeBuild struct {
	item        graph.Edge
	initialized bool
}

func (b *CommitBuilder) Apply(row ParquetCommitRow) error {
	if row.ParentOrdinal < 0 || row.ChildOrdinal < 0 || row.NestedOrdinal < 0 {
		return fmt.Errorf("negative commit ordinal")
	}
	switch row.Kind {
	case CommitRowMetadata:
		return nil
	case "identity_encoding":
		if row.Name != "1" || b.commit.IdentityEncoding != 0 {
			return fmt.Errorf("invalid commit identity encoding %q", row.Name)
		}
		b.commit.IdentityEncoding = graph.IdentityEncodingTuple
	case CommitRowUpsertCIType, CommitRowCITypeExtends, CommitRowCITypeField, CommitRowCITypeFieldEnum, CommitRowCITypeFieldDefault, CommitRowCITypeIdentity, CommitRowCITypeIdentityField:
		return b.applyCITypeRow(row)
	case commitRowDeleteCIType:
		if row.ParentOrdinal > len(b.commit.Mutations.DeleteCITypes) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.DeleteCITypes = SetStringAt(b.commit.Mutations.DeleteCITypes, row.ParentOrdinal, row.Name)
	case CommitRowUpsertRelationType, CommitRowRelationFromKind, CommitRowRelationToKind:
		return b.applyRelationRow(row)
	case commitRowDeleteRelationType:
		if row.ParentOrdinal > len(b.commit.Mutations.DeleteRelationTypes) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.DeleteRelationTypes = SetStringAt(b.commit.Mutations.DeleteRelationTypes, row.ParentOrdinal, row.Name)
	case CommitRowUpsertEntity:
		entity, err := b.entity(row.ParentOrdinal)
		if err != nil {
			return err
		}
		return entity.apply(row, b.entityOrdinals)
	case CommitRowDeleteEntity:
		if row.ParentOrdinal > len(b.commit.Mutations.DeleteEntities) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.DeleteEntities = SetStringAt(b.commit.Mutations.DeleteEntities, row.ParentOrdinal, row.ID)
	case commitRowDeleteEntityRequest:
		if row.ParentOrdinal > len(b.commit.Mutations.DeleteEntityRequests) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.DeleteEntityRequests = setEntityDeleteRequestAt(b.commit.Mutations.DeleteEntityRequests, row.ParentOrdinal, graph.EntityDeleteRequest{
			ID: row.ID, Kind: row.KindName, Source: row.Source, ExternalID: row.ExternalID, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		})
	case commitRowMarkSourceStale:
		if row.ParentOrdinal > len(b.commit.Mutations.MarkSourceStale) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.MarkSourceStale = setSourceStaleAt(b.commit.Mutations.MarkSourceStale, row.ParentOrdinal, graph.SourceStaleRequest{
			Source: row.Source, Kind: row.KindName, Action: row.Action, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		})
	case commitRowStaleObservedExternal:
		value, err := StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		if row.ParentOrdinal >= len(b.commit.Mutations.MarkSourceStale) {
			return fmt.Errorf("missing parent for %q", row.Kind)
		}
		if row.ChildOrdinal > len(b.commit.Mutations.MarkSourceStale[row.ParentOrdinal].ObservedExternalIDs) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.MarkSourceStale[row.ParentOrdinal].ObservedExternalIDs = SetStringAt(b.commit.Mutations.MarkSourceStale[row.ParentOrdinal].ObservedExternalIDs, row.ChildOrdinal, value)
	case CommitRowUpsertEdge:
		edge, err := b.edge(row.ParentOrdinal)
		if err != nil {
			return err
		}
		return edge.apply(row)
	case commitRowDeleteEdge:
		if row.ParentOrdinal > len(b.commit.Mutations.DeleteEdges) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.DeleteEdges = SetStringAt(b.commit.Mutations.DeleteEdges, row.ParentOrdinal, row.ID)
	case commitRowDeleteEdgeRequest:
		if row.ParentOrdinal > len(b.commit.Mutations.DeleteEdgeRequests) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.DeleteEdgeRequests = setEdgeDeleteRequestAt(b.commit.Mutations.DeleteEdgeRequests, row.ParentOrdinal, graph.EdgeDeleteRequest{
			ID: row.ID, Type: row.TypeName, From: row.From, To: row.To, Source: row.Source, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		})
	case commitRowMergeEntity:
		if row.ParentOrdinal > len(b.commit.Mutations.MergeEntities) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.MergeEntities = setMergeAt(b.commit.Mutations.MergeEntities, row.ParentOrdinal, graph.MergeRequest{TargetID: row.TargetID})
	case commitRowMergeSource:
		if row.ParentOrdinal >= len(b.commit.Mutations.MergeEntities) {
			return fmt.Errorf("missing parent for %q", row.Kind)
		}
		if row.ChildOrdinal > len(b.commit.Mutations.MergeEntities[row.ParentOrdinal].SourceIDs) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.MergeEntities[row.ParentOrdinal].SourceIDs = SetStringAt(b.commit.Mutations.MergeEntities[row.ParentOrdinal].SourceIDs, row.ChildOrdinal, row.SourceID)
	case commitRowSplitRequest:
		if row.ParentOrdinal > len(b.commit.Mutations.SplitEntities) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		b.commit.Mutations.SplitEntities = setSplitAt(b.commit.Mutations.SplitEntities, row.ParentOrdinal, graph.SplitRequest{SourceID: row.SourceID})
	case commitRowSplitEntity:
		entity, err := b.splitEntity(row.ParentOrdinal, row.ChildOrdinal)
		if err != nil {
			return err
		}
		return entity.apply(row, b.entityOrdinals)
	default:
		return fmt.Errorf("unknown commit row kind %q", row.Kind)
	}
	return nil
}

func (b *CommitBuilder) FinishMutations() (graph.Mutations, error) {
	for ordinal, item := range b.CITypes() {
		if ordinal != len(b.commit.Mutations.UpsertCITypes) {
			return graph.Mutations{}, fmt.Errorf("non-contiguous ordinal for UpsertCITypes")
		}
		b.commit.Mutations.UpsertCITypes = append(b.commit.Mutations.UpsertCITypes, item)
	}
	for ordinal, item := range b.Relations() {
		if ordinal != len(b.commit.Mutations.UpsertRelationTypes) {
			return graph.Mutations{}, fmt.Errorf("non-contiguous ordinal for UpsertRelationTypes")
		}
		b.commit.Mutations.UpsertRelationTypes = append(b.commit.Mutations.UpsertRelationTypes, item)
	}
	for ordinal, item := range b.Entities() {
		if ordinal != len(b.commit.Mutations.UpsertEntities) {
			return graph.Mutations{}, fmt.Errorf("non-contiguous ordinal for UpsertEntities")
		}
		b.commit.Mutations.UpsertEntities = append(b.commit.Mutations.UpsertEntities, item)
	}
	for ordinal, item := range b.Edges() {
		if ordinal != len(b.commit.Mutations.UpsertEdges) {
			return graph.Mutations{}, fmt.Errorf("non-contiguous ordinal for UpsertEdges")
		}
		b.commit.Mutations.UpsertEdges = append(b.commit.Mutations.UpsertEdges, item)
	}
	for _, parent := range SortedIntKeys(b.splitEntities) {
		if parent >= len(b.commit.Mutations.SplitEntities) {
			return graph.Mutations{}, fmt.Errorf("missing split request")
		}
		request := &b.commit.Mutations.SplitEntities[parent]
		for _, child := range SortedIntKeys(b.splitEntities[parent]) {
			if child != len(request.Entities) {
				return graph.Mutations{}, fmt.Errorf("non-contiguous split entity ordinal")
			}
			request.Entities = append(request.Entities, DecodedEntityCopy(b.splitEntities[parent][child].item))
		}
	}
	return b.commit.Mutations, nil
}

func (b *CommitBuilder) finish() (CommitTableItem, error) {
	mutations, err := b.FinishMutations()
	if err != nil {
		return CommitTableItem{}, err
	}
	b.commit.Mutations = mutations
	version, err := ReadableLayoutVersion("commit", b.commit.LayoutVersion)
	if err != nil {
		return CommitTableItem{}, err
	}
	b.commit.LayoutVersion = version
	hash, err := CommitPayloadHash(b.commit)
	if err != nil {
		return CommitTableItem{}, err
	}
	if b.contentHash == "" || b.contentHash != hash {
		return CommitTableItem{}, fmt.Errorf("commit object content hash mismatch")
	}
	return CommitTableItem{Key: b.key, Commit: b.commit, ContentHash: b.contentHash}, nil
}

func (b *CommitBuilder) ciType(ordinal int) *ciTypeBuild {
	if b.ciTypes == nil {
		b.ciTypes = map[int]*ciTypeBuild{}
	}
	item := b.ciTypes[ordinal]
	if item == nil {
		item = &ciTypeBuild{fields: map[int]string{}, identities: map[int]*identityBuild{}}
		b.ciTypes[ordinal] = item
	}
	return item
}

func (b *CommitBuilder) relation(ordinal int) *relationBuild {
	if b.relations == nil {
		b.relations = map[int]*relationBuild{}
	}
	item := b.relations[ordinal]
	if item == nil {
		item = &relationBuild{}
		b.relations[ordinal] = item
	}
	return item
}

func (b *CommitBuilder) entity(ordinal int) (*entityBuild, error) {
	if b.entities == nil {
		b.entities = map[int]*entityBuild{}
	}
	item := b.entities[ordinal]
	if item == nil {
		item = &entityBuild{}
		b.entities[ordinal] = item
	}
	return item, nil
}

func (b *CommitBuilder) edge(ordinal int) (*edgeBuild, error) {
	if b.edges == nil {
		b.edges = map[int]*edgeBuild{}
	}
	item := b.edges[ordinal]
	if item == nil {
		item = &edgeBuild{}
		b.edges[ordinal] = item
	}
	return item, nil
}

func (b *CommitBuilder) splitEntity(parent int, child int) (*entityBuild, error) {
	if b.splitEntities == nil {
		b.splitEntities = map[int]map[int]*entityBuild{}
	}
	byChild := b.splitEntities[parent]
	if byChild == nil {
		byChild = map[int]*entityBuild{}
		b.splitEntities[parent] = byChild
	}
	item := byChild[child]
	if item == nil {
		item = &entityBuild{}
		byChild[child] = item
	}
	return item, nil
}

func (b *CommitBuilder) applyCITypeRow(row ParquetCommitRow) error {
	item := b.ciType(row.ParentOrdinal)
	switch row.Kind {
	case CommitRowUpsertCIType:
		item.item.Name = row.Name
		item.item.DisplayName = row.DisplayName
	case CommitRowCITypeExtends:
		value, err := StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		if row.ChildOrdinal > len(item.item.Extends) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		item.item.Extends = SetStringAt(item.item.Extends, row.ChildOrdinal, value)
	case CommitRowCITypeField:
		if item.item.Fields == nil {
			item.item.Fields = map[string]graph.FieldSpec{}
		}
		item.fields[row.ChildOrdinal] = row.EntryKey
		item.item.Fields[row.EntryKey] = graph.FieldSpec{Type: row.FieldType, MergeStrategy: row.Strategy, Required: row.Required, Indexed: row.Indexed, Unique: row.Unique}
	case CommitRowCITypeFieldEnum:
		field, ok := item.fields[row.ChildOrdinal]
		if !ok || item.item.Fields == nil {
			return fmt.Errorf("missing parent field ordinal %d for %q", row.ChildOrdinal, row.Kind)
		}
		spec := item.item.Fields[field]
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		if row.NestedOrdinal > len(spec.Enum) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		spec.Enum = setAnyAt(spec.Enum, row.NestedOrdinal, value)
		item.item.Fields[field] = spec
	case CommitRowCITypeFieldDefault:
		field, ok := item.fields[row.ChildOrdinal]
		if !ok || item.item.Fields == nil {
			return fmt.Errorf("missing parent field ordinal %d for %q", row.ChildOrdinal, row.Kind)
		}
		spec := item.item.Fields[field]
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		spec.Default = value
		item.item.Fields[field] = spec
	case CommitRowCITypeIdentity:
		identity := graph.IdentityKey{Name: row.Name, ConfidenceThreshold: row.Confidence, Strategy: row.Strategy}
		if row.ChildOrdinal > len(item.item.IdentityKeys) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		item.item.IdentityKeys = setIdentityAt(item.item.IdentityKeys, row.ChildOrdinal, identity)
	case CommitRowCITypeIdentityField:
		value, err := StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		if row.ChildOrdinal >= len(item.item.IdentityKeys) {
			return fmt.Errorf("missing parent for %q", row.Kind)
		}
		if row.NestedOrdinal > len(item.item.IdentityKeys[row.ChildOrdinal].Fields) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		item.item.IdentityKeys[row.ChildOrdinal].Fields = SetStringAt(item.item.IdentityKeys[row.ChildOrdinal].Fields, row.NestedOrdinal, value)
	}
	return nil
}

func (b *CommitBuilder) applyRelationRow(row ParquetCommitRow) error {
	item := b.relation(row.ParentOrdinal)
	switch row.Kind {
	case CommitRowUpsertRelationType:
		item.item = graph.RelationType{
			Name: row.Name, DisplayName: row.DisplayName, ReverseName: row.ReverseName, FromKind: row.FromKind, ToKind: row.ToKind,
			Directed: row.Directed, Cardinality: row.Cardinality, ImpactDirection: row.ImpactDirection, AllowCrossKind: row.AllowCrossKind, Standard: row.Standard,
		}
	case CommitRowRelationFromKind:
		value, err := StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		if row.ChildOrdinal > len(item.item.FromKinds) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		item.item.FromKinds = SetStringAt(item.item.FromKinds, row.ChildOrdinal, value)
	case CommitRowRelationToKind:
		value, err := StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		if row.ChildOrdinal > len(item.item.ToKinds) {
			return fmt.Errorf("non-contiguous commit ordinal for %q", row.Kind)
		}
		item.item.ToKinds = SetStringAt(item.item.ToKinds, row.ChildOrdinal, value)
	}
	return nil
}

func (b *entityBuild) apply(row ParquetCommitRow, remaining *int64) error {
	if !b.initialized {
		b.item = graph.Entity{
			ID: row.ID, Kind: row.KindName, Source: row.Source, ExternalID: row.ExternalID, Confidence: row.Confidence, SourceRank: row.SourcePriority,
			Version: row.VersionValue, CreatedAt: row.CreatedAtValue, UpdatedAt: row.UpdatedAtValue, SplitFrom: row.SplitFrom,
		}
		b.initialized = true
	}
	return ApplyEntityRow(&b.item, EntityRow{
		Kind: row.ComponentKind, Ordinal: row.NestedOrdinal, Key: row.EntryKey, Value: row.Value, Strategy: row.Strategy, FieldSource: row.FieldSource, EntitySource: row.EntitySource,
	}, remaining)
}

func (b *edgeBuild) apply(row ParquetCommitRow) error {
	if !b.initialized {
		b.item = graph.Edge{
			ID: row.ID, Type: row.TypeName, From: row.From, To: row.To, Source: row.Source, ExternalID: row.ExternalID, Confidence: row.Confidence, SourceRank: row.SourcePriority,
			Version: row.VersionValue, CreatedAt: row.CreatedAtValue, UpdatedAt: row.UpdatedAtValue,
		}
		b.initialized = true
	}
	return ApplyEdgeRow(&b.item, EdgeRow{
		Kind: row.ComponentKind, Ordinal: row.ChildOrdinal, Key: row.EntryKey, Value: row.Value, FieldSource: row.FieldSource, EdgeSource: row.EdgeSource,
	})
}
