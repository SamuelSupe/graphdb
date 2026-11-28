package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/backend"

type MemoryStore = backend.MemoryStore

func NewMemoryStore() *MemoryStore {
	return backend.NewMemoryStore()
}
