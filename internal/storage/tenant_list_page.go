package storage

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// TenantListOptions selects a bounded, ID-ordered page. A zero Limit preserves
// the original unpaged API; Cursor is the last returned tenant ID, not an offset.
type TenantListOptions struct {
	Limit         int
	Query         string
	Cursor        string
	IncludeLegacy bool
}

func ParseTenantListOptions(values url.Values) (TenantListOptions, error) {
	options := TenantListOptions{Query: strings.TrimSpace(values.Get("q")), Cursor: values.Get("cursor"), IncludeLegacy: values.Get("include_legacy") == "true"}
	if values.Has("limit") || values.Has("q") || values.Has("cursor") {
		options.Limit = 50
		if values.Has("limit") {
			limit, err := strconv.Atoi(values.Get("limit"))
			if err != nil || limit < 1 || limit > 200 {
				return options, fmt.Errorf("limit must be between 1 and 200")
			}
			options.Limit = limit
		}
	}
	if len(options.Query) > 128 {
		return options, fmt.Errorf("q must be at most 128 bytes")
	}
	if options.Cursor != "" {
		if err := ValidateTenantID(options.Cursor); err != nil {
			return options, fmt.Errorf("invalid tenant cursor: %w", err)
		}
	}
	return options, nil
}

// PageTenantIDs reads an already sorted registry/catalog. Filtering is a
// case-insensitive ID substring match. Only the selected page needs metadata.
// Pages are live: new IDs before Cursor are visible after restarting at page one.
func PageTenantIDs(ids []string, options TenantListOptions) (page []string, total int, next string) {
	limit := options.Limit
	if limit <= 0 {
		limit = len(ids)
	}
	page = make([]string, 0, limit)
	query := strings.ToLower(options.Query)
	for _, id := range ids {
		if !strings.Contains(strings.ToLower(id), query) {
			continue
		}
		total++
		if id <= options.Cursor {
			continue
		}
		if len(page) < limit {
			page = append(page, id)
		} else if next == "" {
			next = page[len(page)-1]
		}
	}
	return page, total, next
}

type TenantInfoPage struct {
	Tenants    []TenantInfo `json:"tenants"`
	Total      int          `json:"total"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// ListTenantInfoPage reads the registry once and loads details only for the page.
// IncludeLegacy explicitly opts into a full object-prefix discovery scan.
// ponytail: the ID registry still reads O(n); partition it only when that I/O is the bottleneck.
func (s *TenantStore) ListTenantInfoPage(ctx context.Context, options TenantListOptions) (TenantInfoPage, error) {
	var ids []string
	var err error
	if options.IncludeLegacy {
		ids, err = s.ListTenantsIncludingLegacy(ctx)
	} else {
		ids, err = s.ListManagedTenants(ctx)
	}
	if err != nil {
		return TenantInfoPage{}, err
	}
	page, total, next := PageTenantIDs(ids, options)
	items, err := s.tenantInfos(ctx, page)
	return TenantInfoPage{Tenants: items, Total: total, NextCursor: next}, err
}
