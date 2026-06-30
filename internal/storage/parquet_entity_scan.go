package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

var parquetEntityCandidateColumns = []int{
	parquetEntityColumnID,
	parquetEntityColumnKind,
	parquetEntityColumnSource,
	parquetEntityColumnRowKind,
	parquetEntityColumnFieldSourceSource,
	parquetEntityColumnEntitySourceSource,
}

type parquetEntityCandidateScan struct {
	IDs              map[string]struct{}
	RowGroupsRead    int
	RowGroupsSkipped int
}

type parquetEntityCandidateState struct {
	baseSet  bool
	baseOK   bool
	sourceOK bool
}

func shouldScanParquetEntityCandidates(options EntityScanOptions, cursor scanCursor) bool {
	return options.Kind != "" || options.Source != "" || cursor.After != ""
}

func scanParquetEntityPageCandidates(ctx context.Context, data []byte, shard string, options EntityScanOptions, cursor scanCursor) (parquetEntityCandidateScan, error) {
	out := parquetEntityCandidateScan{IDs: map[string]struct{}{}}
	reader, err := pqfile.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return out, err
	}
	defer reader.Close()

	rowGroups := parquetEntityCandidateRowGroups(reader, options)
	out.RowGroupsRead = len(rowGroups)
	out.RowGroupsSkipped = reader.NumRowGroups() - len(rowGroups)
	if len(rowGroups) == 0 {
		return out, objectContextErr(ctx)
	}
	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 4096}, memory.DefaultAllocator)
	if err != nil {
		return out, err
	}
	table, err := fileReader.ReadRowGroups(ctx, parquetEntityCandidateColumns, rowGroups)
	if err != nil {
		return out, err
	}
	defer table.Release()
	if table.NumCols() < int64(len(parquetEntityCandidateColumns)) {
		return out, fmt.Errorf("parquet entity candidate scan has %d columns, want %d", table.NumCols(), len(parquetEntityCandidateColumns))
	}

	states := map[string]*parquetEntityCandidateState{}
	tableReader := array.NewTableReader(table, 4096)
	defer tableReader.Release()
	for tableReader.Next() {
		record := tableReader.RecordBatch()
		if record.NumCols() < int64(len(parquetEntityCandidateColumns)) {
			return out, fmt.Errorf("parquet entity candidate record has %d columns, want %d", record.NumCols(), len(parquetEntityCandidateColumns))
		}
		ids, err := parquetStringColumn(record, 0, "entity_id")
		if err != nil {
			return out, err
		}
		kinds, err := parquetStringColumn(record, 1, "kind")
		if err != nil {
			return out, err
		}
		sources, err := parquetStringColumn(record, 2, "source")
		if err != nil {
			return out, err
		}
		rowKinds, err := parquetStringColumn(record, 3, "row_kind")
		if err != nil {
			return out, err
		}
		fieldSources, err := parquetStringColumn(record, 4, "field_source_source")
		if err != nil {
			return out, err
		}
		entitySources, err := parquetStringColumn(record, 5, "entity_source_source")
		if err != nil {
			return out, err
		}
		for row := 0; row < int(record.NumRows()); row++ {
			id := ids.Value(row)
			if id == "" {
				continue
			}
			state := states[id]
			if state == nil {
				state = &parquetEntityCandidateState{}
				states[id] = state
			}
			if !state.baseSet {
				state.baseSet = true
				state.baseOK = scanKey(shard, id) > cursor.After && (options.Kind == "" || kinds.Value(row) == options.Kind)
			}
			if options.Source == "" {
				state.sourceOK = true
				continue
			}
			if sources.Value(row) == options.Source {
				state.sourceOK = true
				continue
			}
			switch rowKinds.Value(row) {
			case entityPageRowSource:
				if entitySources.Value(row) == options.Source {
					state.sourceOK = true
				}
			case entityPageRowExistenceSource:
				if fieldSources.Value(row) == options.Source {
					state.sourceOK = true
				}
			}
		}
	}
	if err := tableReader.Err(); err != nil {
		return out, err
	}
	for id, state := range states {
		if state.baseOK && state.sourceOK {
			out.IDs[id] = struct{}{}
		}
	}
	return out, objectContextErr(ctx)
}

func parquetEntityCandidateRowGroups(reader *pqfile.Reader, options EntityScanOptions) []int {
	rowGroups := make([]int, 0, reader.NumRowGroups())
	for i := 0; i < reader.NumRowGroups(); i++ {
		if options.Kind != "" && !parquetRowGroupStringMayContain(reader, i, parquetEntityColumnKind, options.Kind) {
			continue
		}
		if options.Source != "" &&
			!parquetRowGroupStringMayContain(reader, i, parquetEntityColumnSource, options.Source) &&
			!parquetRowGroupStringMayContain(reader, i, parquetEntityColumnEntitySourceSource, options.Source) &&
			!parquetRowGroupStringMayContain(reader, i, parquetEntityColumnFieldSourceSource, options.Source) {
			continue
		}
		rowGroups = append(rowGroups, i)
	}
	return rowGroups
}

func parquetRowGroupStringMayContain(reader *pqfile.Reader, rowGroup int, column int, value string) bool {
	if value == "" {
		return true
	}
	group := reader.MetaData().RowGroup(rowGroup)
	chunk, err := group.ColumnChunk(column)
	if err != nil {
		return true
	}
	stats, err := chunk.Statistics()
	if err != nil || stats == nil || !stats.HasMinMax() {
		return true
	}
	minValue := string(stats.EncodeMin())
	maxValue := string(stats.EncodeMax())
	return value >= minValue && value <= maxValue
}
