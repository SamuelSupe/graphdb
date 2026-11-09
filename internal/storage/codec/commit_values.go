package codec

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
)

func commitBuildKey(key string, id string, version int64) string {
	return key + "\x00" + id + "\x00" + strconv.FormatInt(version, 10)
}

func StringCommitValue(value string) parquetio.Value {
	return parquetio.Value{Kind: parquetio.ParquetValueKindString, StringValue: value}
}

func StringFromCommitValue(value parquetio.Value) (string, error) {
	out, err := parquetio.AnyFromValue(value)
	if err != nil {
		return "", err
	}
	if out == nil {
		return "", nil
	}
	text, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("commit value is %T, want string", out)
	}
	return text, nil
}

func sortedFieldSpecKeys(values map[string]graph.FieldSpec) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func SortedIntKeys[T any](values map[int]T) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func SetStringAt(values []string, index int, value string) []string {
	for len(values) <= index {
		values = append(values, "")
	}
	values[index] = value
	return values
}

func setAnyAt(values []any, index int, value any) []any {
	for len(values) <= index {
		values = append(values, nil)
	}
	values[index] = value
	return values
}

func SetCITypeAt(values []graph.CIType, index int, value graph.CIType) []graph.CIType {
	for len(values) <= index {
		values = append(values, graph.CIType{})
	}
	values[index] = value
	return values
}

func SetRelationTypeAt(values []graph.RelationType, index int, value graph.RelationType) []graph.RelationType {
	for len(values) <= index {
		values = append(values, graph.RelationType{})
	}
	values[index] = value
	return values
}

func SetEntityAt(values []graph.Entity, index int, value graph.Entity) []graph.Entity {
	for len(values) <= index {
		values = append(values, graph.Entity{})
	}
	values[index] = value
	return values
}

func SetEdgeAt(values []graph.Edge, index int, value graph.Edge) []graph.Edge {
	for len(values) <= index {
		values = append(values, graph.Edge{})
	}
	values[index] = value
	return values
}

func ensureIdentityLen(values []graph.IdentityKey, index int) []graph.IdentityKey {
	for len(values) <= index {
		values = append(values, graph.IdentityKey{})
	}
	return values
}

func setIdentityAt(values []graph.IdentityKey, index int, value graph.IdentityKey) []graph.IdentityKey {
	values = ensureIdentityLen(values, index)
	values[index] = value
	return values
}

func ensureSourceStaleLen(values []graph.SourceStaleRequest, index int) []graph.SourceStaleRequest {
	for len(values) <= index {
		values = append(values, graph.SourceStaleRequest{})
	}
	return values
}

func setSourceStaleAt(values []graph.SourceStaleRequest, index int, value graph.SourceStaleRequest) []graph.SourceStaleRequest {
	values = ensureSourceStaleLen(values, index)
	values[index] = value
	return values
}

func setEntityDeleteRequestAt(values []graph.EntityDeleteRequest, index int, value graph.EntityDeleteRequest) []graph.EntityDeleteRequest {
	for len(values) <= index {
		values = append(values, graph.EntityDeleteRequest{})
	}
	values[index] = value
	return values
}

func setEdgeDeleteRequestAt(values []graph.EdgeDeleteRequest, index int, value graph.EdgeDeleteRequest) []graph.EdgeDeleteRequest {
	for len(values) <= index {
		values = append(values, graph.EdgeDeleteRequest{})
	}
	values[index] = value
	return values
}

func ensureMergeLen(values []graph.MergeRequest, index int) []graph.MergeRequest {
	for len(values) <= index {
		values = append(values, graph.MergeRequest{})
	}
	return values
}

func setMergeAt(values []graph.MergeRequest, index int, value graph.MergeRequest) []graph.MergeRequest {
	values = ensureMergeLen(values, index)
	values[index] = value
	return values
}

func ensureSplitLen(values []graph.SplitRequest, index int) []graph.SplitRequest {
	for len(values) <= index {
		values = append(values, graph.SplitRequest{})
	}
	return values
}

func setSplitAt(values []graph.SplitRequest, index int, value graph.SplitRequest) []graph.SplitRequest {
	values = ensureSplitLen(values, index)
	values[index] = value
	return values
}
