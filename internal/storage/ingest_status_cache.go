package storage

import "encoding/json"

type ingestFailedStatus struct {
	key     string
	pending *ingestPending
	bytes   int64
}

func (s *IngestService) cacheFailedStatusLocked(key string, pending *ingestPending) {
	data, err := json.Marshal(statusFromPending(pending, s.config.OwnerID, s.config.WAL.Durability))
	size := int64(len(data) + len(key) + 512)
	limit := min(s.config.QueueMemoryBytes, int64(4<<20))
	if err != nil || size > limit {
		delete(s.activeByStatus, key)
		return
	}
	for len(s.failedStatuses) > 0 && (len(s.failedStatuses) >= 1024 || s.failedStatusBytes+size > limit) {
		oldest := s.failedStatuses[0]
		s.failedStatuses[0] = ingestFailedStatus{}
		s.failedStatuses = s.failedStatuses[1:]
		s.failedStatusBytes -= oldest.bytes
		if s.activeByStatus[oldest.key] == oldest.pending {
			delete(s.activeByStatus, oldest.key)
		}
	}
	s.failedStatuses = append(s.failedStatuses, ingestFailedStatus{key: key, pending: pending, bytes: size})
	s.failedStatusBytes += size
}
