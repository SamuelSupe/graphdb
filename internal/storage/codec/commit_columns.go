package codec

import "strings"

func (c parquetCommitColumnSet) TenantIDAt(row int) string {
	return strings.Clone(c.tenantID.Value(row))
}

func (c parquetCommitColumnSet) CommitTenantIDAt(row int) string {
	return strings.Clone(c.commitTenantID.Value(row))
}

func (c parquetCommitColumnSet) CommitIDAt(row int) string {
	return strings.Clone(c.commitID.Value(row))
}

func (c parquetCommitColumnSet) VersionAt(row int) int64 { return c.version.Value(row) }

func (c parquetCommitColumnSet) ContentHashAt(row int) string {
	return strings.Clone(c.contentHash.Value(row))
}

func (c parquetCommitColumnSet) CreatedTimeAt(row int) string { return c.createdAt.Value(row) }
