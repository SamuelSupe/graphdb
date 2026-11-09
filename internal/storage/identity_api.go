package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/identity"

func ValidateTenantID(tenantID string) error { return identity.ValidateTenantID(tenantID) }
