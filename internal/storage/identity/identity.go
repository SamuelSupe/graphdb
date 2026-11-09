// Package identity validates tenant IDs and encodes storage-path identities.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func ValidateTenantID(tenantID string) error {
	if !tenantIDPattern.MatchString(tenantID) || strings.Contains(tenantID, "..") {
		return fmt.Errorf("invalid tenant id %q", tenantID)
	}
	return nil
}

func ObjectSegment(value string) string {
	escaped := url.PathEscape(strings.TrimSpace(value))
	switch escaped {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return escaped
	}
}

func NewCommitID() (string, error) {
	var bytes [12]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
