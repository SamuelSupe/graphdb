package storage

import "time"

type IngestObserver interface {
	RecordIngestWALAppend(recordType string, status string, bytes int, duration time.Duration)
	RecordIngestWALSync(status string, records int, bytes int, duration time.Duration)
	RecordIngestWALState(bufferBytes int, diskBytes int64, writtenLSN uint64, durableLSN uint64)
	RecordIngestQueue(pending int, bytes int64, oldest time.Duration)
	RecordIngestQueueCache(event string)
	RecordIngestFlush(
		status string,
		duration time.Duration,
		requests int,
		logicalCommits int,
		segments int,
		manifestPublishes int,
		exactDedup int,
		fallback bool,
	)
	RecordIngestRecovery(status string, records int, pending int, prepared int, duration time.Duration)
	RecordIngestMetadataQueue(pending int, bytes int64, oldest time.Duration)
	RecordIngestMetadataFlush(
		status string,
		duration time.Duration,
		requests int,
		segmentBytes int,
		segmentPuts int,
		manifestPublishes int,
		manifestConflicts int,
		indexPublishes int,
	)
	RecordIngestMetadataLookup(kind string, outcome string, candidates int, duration time.Duration)
	RecordIngestMetadataReplay(bytes int64)
}

type IngestLogger interface {
	Info(event string, fields map[string]any)
	Error(event string, fields map[string]any)
}
