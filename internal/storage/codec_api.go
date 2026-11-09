package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/codec"

type (
	commitBuild            = codec.CommitBuilder
	edgeShardRow           = codec.EdgeRow
	entityPageRow          = codec.EntityRow
	legacyEntity           = codec.LegacyEntity
	parquetCommitRow       = codec.ParquetCommitRow
	parquetCommitTableItem = codec.CommitTableItem
)

const (
	CurrentObjectLayoutVersion              = codec.CurrentObjectLayoutVersion
	LegacyObjectLayoutVersion               = codec.LegacyObjectLayoutVersion
	commitRowCITypeExtends                  = codec.CommitRowCITypeExtends
	commitRowCITypeField                    = codec.CommitRowCITypeField
	commitRowCITypeFieldDefault             = codec.CommitRowCITypeFieldDefault
	commitRowCITypeFieldEnum                = codec.CommitRowCITypeFieldEnum
	commitRowCITypeIdentity                 = codec.CommitRowCITypeIdentity
	commitRowCITypeIdentityField            = codec.CommitRowCITypeIdentityField
	commitRowDeleteEntity                   = codec.CommitRowDeleteEntity
	commitRowMetadata                       = codec.CommitRowMetadata
	commitRowRelationFromKind               = codec.CommitRowRelationFromKind
	commitRowRelationToKind                 = codec.CommitRowRelationToKind
	commitRowUpsertCIType                   = codec.CommitRowUpsertCIType
	commitRowUpsertEdge                     = codec.CommitRowUpsertEdge
	commitRowUpsertEntity                   = codec.CommitRowUpsertEntity
	commitRowUpsertRelationType             = codec.CommitRowUpsertRelationType
	entityPageRowExistenceSource            = codec.EntityPageRowExistenceSource
	entityPageRowField                      = codec.EntityPageRowField
	entityPageRowMetadata                   = codec.EntityPageRowMetadata
	entityPageRowSource                     = codec.EntityPageRowSource
	parquetCommitColumnEdgeSourceObservedAt = codec.ParquetCommitColumnEdgeSourceObservedAt
)
