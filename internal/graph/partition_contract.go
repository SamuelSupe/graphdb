package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// PartitionEntityReferences uses the same normalization, identity confidence
// thresholds and inherited unique fields as commit validation. The returned
// signatures identify authoritative distributed constraint records.
func (g *Graph) PartitionEntityReferences(entity Entity, incoming bool) (Entity, []string, error) {
	var err error
	if incoming {
		entity, err = normalizeEntity(entity)
		if err != nil {
			return Entity{}, nil, err
		}
		sanitizeIncomingEntitySources(&entity)
		fields, err := g.EffectiveFields(entity.Kind)
		if err != nil {
			return Entity{}, nil, err
		}
		if err := g.applyEntitySchemaWithSpecs(&entity, fields); err != nil {
			return Entity{}, nil, err
		}
		if entity.ID == "" {
			entity.ID = canonicalEntityID(entity)
		}
	}
	signatures := g.identitySignatures(entity)
	if incoming {
		signatures = g.lookupIdentitySignatures(entity)
	}
	refs := make([]string, 0, len(signatures))
	for _, signature := range signatures {
		refs = append(refs, identityTuple("identity", entity.Kind, signature.Value))
	}
	fields, err := g.EffectiveFields(entity.Kind)
	if err != nil {
		return Entity{}, nil, err
	}
	for field, spec := range fields {
		if !spec.Unique {
			continue
		}
		value, ok := entity.Fields[field]
		if !ok {
			continue
		}
		key, scalar := ScalarIndexKey(value)
		if !scalar {
			encoded, err := json.Marshal(value)
			if err != nil {
				return Entity{}, nil, err
			}
			key = "json:" + string(encoded)
		}
		refs = append(refs, identityTuple("unique", entity.Kind, field, key))
	}
	return entity, refs, nil
}

// PartitionEntityAliases returns stored merged identities used as references.
func PartitionEntityAliases(entity Entity) []string { return entityAliasValues(entity) }

// PartitionEdgeAliases returns stored historical edge identities.
func PartitionEdgeAliases(edge Edge) []string { return edgeAliasValues(edge) }

// LogicalItemDigest exposes the persisted logical leaf contract without
// constructing an in-memory graph for a distributed fragment.
func LogicalItemDigest(category int, key string, value any) (uint8, string, error) {
	if category < 0 || category >= len(logicalHashKinds) {
		return 0, "", fmt.Errorf("invalid logical category")
	}
	switch category {
	case 1:
		entity, ok := value.(Entity)
		if !ok {
			return 0, "", fmt.Errorf("entity logical value required")
		}
		value = logicalEntityForHash(entity)
	case 3:
		edge, ok := value.(Edge)
		if !ok {
			return 0, "", fmt.Errorf("edge logical value required")
		}
		value = logicalEdgeForHash(edge)
	}
	entry, err := hashLogicalEntry(category, key, value)
	if err != nil {
		return 0, "", err
	}
	return logicalHashShard(key), hex.EncodeToString(entry.digest[:]), nil
}

// ContentHashFromBuckets preserves sha256-shards-v2 category and bucket order.
// Empty strings denote empty buckets, whose digest is all zero bytes.
func ContentHashFromBuckets(buckets [1024]string) (string, error) {
	h := sha256.New()
	_, _ = h.Write([]byte(ContentHashAlgorithm + "\x00"))
	var empty [sha256.Size]byte
	for _, bucket := range buckets {
		if bucket == "" {
			_, _ = h.Write(empty[:])
			continue
		}
		value, err := hex.DecodeString(bucket)
		if err != nil || len(value) != sha256.Size {
			return "", fmt.Errorf("invalid logical bucket digest")
		}
		_, _ = h.Write(value)
	}
	return ContentHashAlgorithm + ":" + hex.EncodeToString(h.Sum(nil)), nil
}
