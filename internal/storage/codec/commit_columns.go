package codec

import (
	"fmt"
	"strings"
)

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

func (c parquetCommitColumnSet) ValidateOrdinals(row int, maxRows int64, childIsIndex bool) error {
	for _, ordinal := range []int64{c.parentOrdinal.Value(row), c.nestedOrdinal.Value(row)} {
		if ordinal < 0 || ordinal >= maxRows {
			return fmt.Errorf("invalid parquet ordinal %d for %d rows", ordinal, maxRows)
		}
	}
	if childIsIndex {
		ordinal := c.childOrdinal.Value(row)
		if ordinal < 0 || ordinal >= maxRows {
			return fmt.Errorf("invalid parquet ordinal %d for %d rows", ordinal, maxRows)
		}
	}
	return nil
}
