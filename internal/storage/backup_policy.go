package storage

import "fmt"

type TenantBackupConfig struct {
	Enabled                     *bool  `json:"enabled,omitempty"`
	IntervalSeconds             *int64 `json:"interval_seconds,omitempty"`
	KeepCount                   *int   `json:"keep_count,omitempty"`
	MaxAgeSeconds               *int64 `json:"max_age_seconds,omitempty"`
	RetryInitialSeconds         *int64 `json:"retry_initial_seconds,omitempty"`
	RetryMaxSeconds             *int64 `json:"retry_max_seconds,omitempty"`
	RestoreDrillIntervalSeconds *int64 `json:"restore_drill_interval_seconds,omitempty"`
}

func backupPolicy(c TenantBackupConfig) TenantBackupConfig {
	enabled := false
	interval := int64(86400)
	keepCount := 30
	maxAge := int64(30 * 86400)
	retryInitial := int64(60)
	retryMax := int64(3600)
	drillInterval := int64(0)

	if c.Enabled == nil {
		c.Enabled = &enabled
	}
	if c.IntervalSeconds == nil {
		c.IntervalSeconds = &interval
	}
	if c.KeepCount == nil {
		c.KeepCount = &keepCount
	}
	if c.MaxAgeSeconds == nil {
		c.MaxAgeSeconds = &maxAge
	}
	if c.RetryInitialSeconds == nil {
		c.RetryInitialSeconds = &retryInitial
	}
	if c.RetryMaxSeconds == nil {
		c.RetryMaxSeconds = &retryMax
	}
	if c.RestoreDrillIntervalSeconds == nil {
		c.RestoreDrillIntervalSeconds = &drillInterval
	}
	return c
}

func validateBackupPolicy(c TenantBackupConfig) error {
	c = backupPolicy(c)
	if *c.IntervalSeconds < 60 || *c.IntervalSeconds > 365*86400 {
		return fmt.Errorf("backup.interval_seconds must be between 60 and 31536000")
	}
	if *c.KeepCount < 0 || *c.KeepCount > 1000 {
		return fmt.Errorf("backup.keep_count must be between 0 and 1000")
	}
	for name, value := range map[string]int64{"max_age_seconds": *c.MaxAgeSeconds, "restore_drill_interval_seconds": *c.RestoreDrillIntervalSeconds} {
		if value < 0 || value > 10*365*86400 {
			return fmt.Errorf("backup.%s must be between 0 and 315360000", name)
		}
	}
	if *c.RetryInitialSeconds < 1 || *c.RetryMaxSeconds < *c.RetryInitialSeconds || *c.RetryMaxSeconds > 86400 {
		return fmt.Errorf("backup retry delay must satisfy 1 <= initial <= max <= 86400 seconds")
	}
	return nil
}
