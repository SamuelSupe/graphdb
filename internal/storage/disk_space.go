package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type DiskSpacePolicy struct {
	MinFreeBytes         int64
	MinFreePercent       int
	MinFreeInodes        int64
	MinFreeInodesPercent int
}

func (p DiskSpacePolicy) enabled() bool {
	return p.MinFreeBytes != 0 || p.MinFreePercent != 0 || p.MinFreeInodes != 0 || p.MinFreeInodesPercent != 0
}

func (s *TenantStore) checkRestoreDiskSpace(ctx context.Context, record TenantBackupRecord) error {
	files := s.localFileStore()
	if IsReplicatedContext(ctx) || files == nil || !files.diskPolicy.enabled() {
		return nil
	}
	var size int64
	add := func(value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if int64(len(data)) > math.MaxInt64/4-size {
			return fmt.Errorf("restore disk space estimate is too large")
		}
		size += int64(len(data))
		return nil
	}
	for _, entity := range record.Snapshot.Entities {
		if err := add(entity); err != nil {
			return err
		}
	}
	for _, edge := range record.Snapshot.Edges {
		if err := add(edge); err != nil {
			return err
		}
	}
	return s.checkDiskSpace(ctx, max(8<<20, size*4), false)
}

type DiskSpaceStatus struct {
	FilesystemID      string `json:"filesystem_id,omitempty"`
	TotalBytes        int64  `json:"total_bytes"`
	AvailableBytes    int64  `json:"available_bytes"`
	MinimumFreeBytes  int64  `json:"minimum_free_bytes"`
	TotalInodes       int64  `json:"total_inodes"`
	AvailableInodes   int64  `json:"available_inodes"`
	MinimumFreeInodes int64  `json:"minimum_free_inodes"`
	InodesKnown       bool   `json:"inodes_known"`
	WriteReady        bool   `json:"write_ready"`
}

func (p DiskSpacePolicy) Validate() error {
	if p.MinFreeBytes < 0 || p.MinFreePercent < 0 || p.MinFreePercent >= 100 {
		return fmt.Errorf("disk minimum free bytes must be non-negative and percent must be between 0 and 99")
	}
	if p.MinFreeInodes < 0 || p.MinFreeInodesPercent < 0 || p.MinFreeInodesPercent >= 100 {
		return fmt.Errorf("disk minimum free inodes must be non-negative and inode percent must be between 0 and 99")
	}
	return nil
}

func (s *FileStore) ConfigureDiskSpace(p DiskSpacePolicy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	s.diskPolicy = p
	return nil
}

func (s *FileStore) DiskSpace(ctx context.Context) (DiskSpaceStatus, error) {
	return inspectDiskSpace(ctx, s.root, s.diskPolicy, s.diskProbe)
}

func InspectDiskSpace(ctx context.Context, path string, policy DiskSpacePolicy) (DiskSpaceStatus, error) {
	if err := policy.Validate(); err != nil {
		return DiskSpaceStatus{}, err
	}
	return inspectDiskSpace(ctx, path, policy, nil)
}

func CheckDiskSpace(ctx context.Context, path string, policy DiskSpacePolicy, additionalBytes int64) error {
	if !policy.enabled() {
		return nil
	}
	status, err := InspectDiskSpace(ctx, path, policy)
	if err != nil {
		return err
	}
	return checkAvailableDiskSpace(status, additionalBytes, false)
}

func inspectDiskSpace(ctx context.Context, path string, policy DiskSpacePolicy, probe func(string) (DiskSpaceStatus, error)) (DiskSpaceStatus, error) {
	if err := ctx.Err(); err != nil {
		return DiskSpaceStatus{}, err
	}
	if probe == nil {
		probe = filesystemSpace
	}
	status, err := probe(path)
	if err != nil {
		return status, fmt.Errorf("%w: inspect disk space: %v", ErrObjectStoreUnavailable, err)
	}
	status.MinimumFreeBytes = max(policy.MinFreeBytes, status.TotalBytes/100*int64(policy.MinFreePercent))
	status.MinimumFreeInodes = max(policy.MinFreeInodes, status.TotalInodes/100*int64(policy.MinFreeInodesPercent))
	status.WriteReady = status.AvailableBytes >= status.MinimumFreeBytes
	if status.InodesKnown {
		status.WriteReady = status.WriteReady && status.AvailableInodes >= max(1, status.MinimumFreeInodes)
	}
	return status, nil
}

func (s *FileStore) checkDiskSpace(ctx context.Context, additionalBytes int64, accepted bool) error {
	if IsReplicatedContext(ctx) || !s.diskPolicy.enabled() {
		return nil
	}
	status, err := s.DiskSpace(ctx)
	if err != nil {
		if reason, ok := objectStoreUnavailableBackpressureReason(err); ok {
			return &BackpressureError{RetryAfter: 2 * time.Second, Reasons: []BackpressureReason{reason}}
		}
		return err
	}
	return checkAvailableDiskSpace(status, additionalBytes, accepted)
}

func checkAvailableDiskSpace(status DiskSpaceStatus, additionalBytes int64, accepted bool) error {
	minimum := status.MinimumFreeBytes
	minimumInodes := status.MinimumFreeInodes
	// Accepted work can consume the admission reserve to reach a durable terminal
	// result. New requests and maintenance builds cannot consume that reserve.
	if accepted {
		minimum = min(minimum, 64<<20)
		minimumInodes = min(minimumInodes, 64)
	}
	if additionalBytes < 0 || additionalBytes > math.MaxInt64-minimum {
		return fmt.Errorf("invalid disk space estimate")
	}
	needed := minimum + additionalBytes
	if status.AvailableBytes < needed {
		return &BackpressureError{RetryAfter: 2 * time.Second, Reasons: []BackpressureReason{{
			Code: "disk_space_low", Current: float64(status.AvailableBytes), Threshold: float64(needed),
			Message: "insufficient available disk space for admission and temporary files",
		}}}
	}
	if status.InodesKnown && status.AvailableInodes < max(1, minimumInodes) {
		return &BackpressureError{RetryAfter: 2 * time.Second, Reasons: []BackpressureReason{{
			Code: "disk_inodes_low", Current: float64(status.AvailableInodes), Threshold: float64(max(1, minimumInodes)),
			Message: "insufficient available filesystem inodes for admission and temporary files",
		}}}
	}
	return nil
}

func (s *TenantStore) checkDiskSpace(ctx context.Context, additionalBytes int64, accepted bool) error {
	if files := s.localFileStore(); files != nil {
		return files.checkDiskSpace(ctx, additionalBytes, accepted)
	}
	return nil
}

func (s *TenantStore) CheckWriteDiskSpace(ctx context.Context, additionalBytes int64) error {
	return s.checkDiskSpace(ctx, additionalBytes, false)
}

func (s *TenantStore) DiskSpace(ctx context.Context) (DiskSpaceStatus, error) {
	if files := s.localFileStore(); files != nil {
		return files.DiskSpace(ctx)
	}
	return DiskSpaceStatus{WriteReady: true}, nil
}

func (s *TenantStore) CheckAcceptedWALBackpressure(ctx context.Context, tenantID string) error {
	return s.checkAcceptedWALBackpressure(ctx, tenantID, true)
}

func (s *TenantStore) CheckTaskDiskSpace(ctx context.Context, task Task) error {
	files := s.localFileStore()
	if IsReplicatedContext(ctx) || files == nil || !files.diskPolicy.enabled() {
		return nil
	}
	switch task.Type {
	case TaskTypeGC, TaskTypeReplayDeadLetter:
		return nil
	case TaskTypeRepair:
		if !boolTaskParam(task.Params, "apply") {
			return nil
		}
	}
	var usage TenantUsageReport
	var err error
	if s.Backpressure != nil {
		if bytes, ok := s.Backpressure.recentTenantBytes(task.TenantID); ok {
			usage.TotalBytes = bytes
		} else {
			usage, err = s.TenantUsage(ctx, task.TenantID)
		}
	} else {
		usage, err = s.TenantUsage(ctx, task.TenantID)
	}
	if err != nil {
		return err
	}
	if usage.TotalBytes > math.MaxInt64/2 {
		return fmt.Errorf("tenant disk space estimate is too large")
	}
	return s.checkDiskSpace(ctx, max(8<<20, usage.TotalBytes*2), false)
}
