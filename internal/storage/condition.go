package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/backend"

func checkCondition(condition PutCondition, currentETag string, exists bool) error {
	return backend.CheckCondition(condition, currentETag, exists)
}
