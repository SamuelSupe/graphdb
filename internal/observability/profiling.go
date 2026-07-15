package observability

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/DataDog/dd-trace-go/v2/profiler"
)

type DatadogProfilerConfig struct {
	Enabled     bool
	ServiceName string
	Environment string
	Version     string
}

type DatadogProfilerStatus struct {
	Enabled            bool `json:"enabled"`
	DDProfilingEnabled bool `json:"dd_profiling_enabled"`
}

type DatadogProfiler interface {
	SetEnabled(enabled bool) (DatadogProfilerStatus, error)
	Status() DatadogProfilerStatus
}

type DatadogProfilerController struct {
	mu      sync.Mutex
	cfg     DatadogProfilerConfig
	enabled bool
	start   func(...profiler.Option) error
	stop    func()
}

// NewDatadogProfilerController manages only the continuous profiler. It
// intentionally does not import or initialize the Datadog tracer.
func NewDatadogProfilerController(cfg DatadogProfilerConfig) (*DatadogProfilerController, error) {
	controller := newDatadogProfilerController(cfg, profiler.Start, profiler.Stop)
	if cfg.Enabled {
		if _, err := controller.SetEnabled(true); err != nil {
			return nil, err
		}
	}
	return controller, nil
}

func newDatadogProfilerController(cfg DatadogProfilerConfig, start func(...profiler.Option) error, stop func()) *DatadogProfilerController {
	return &DatadogProfilerController{cfg: cfg, start: start, stop: stop}
}

func (c *DatadogProfilerController) SetEnabled(enabled bool) (DatadogProfilerStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !enabled {
		if err := os.Setenv("DD_PROFILING_ENABLED", "false"); err != nil {
			return c.statusLocked(), fmt.Errorf("disable Datadog profiler: %w", err)
		}
		if c.enabled {
			c.stop()
			c.enabled = false
		}
		c.cfg.Enabled = false
		return c.statusLocked(), nil
	}
	if c.enabled {
		return c.statusLocked(), nil
	}

	previous, wasSet := os.LookupEnv("DD_PROFILING_ENABLED")
	if err := os.Setenv("DD_PROFILING_ENABLED", "true"); err != nil {
		return c.statusLocked(), fmt.Errorf("enable Datadog profiler: %w", err)
	}
	if err := c.start(c.options()...); err != nil {
		if wasSet {
			_ = os.Setenv("DD_PROFILING_ENABLED", previous)
		} else {
			_ = os.Unsetenv("DD_PROFILING_ENABLED")
		}
		return c.statusLocked(), fmt.Errorf("start Datadog profiler: %w", err)
	}
	c.enabled = true
	c.cfg.Enabled = true
	return c.statusLocked(), nil
}

func (c *DatadogProfilerController) Status() DatadogProfilerStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statusLocked()
}

func (c *DatadogProfilerController) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled {
		return
	}
	c.stop()
	c.enabled = false
}

func (c *DatadogProfilerController) statusLocked() DatadogProfilerStatus {
	return DatadogProfilerStatus{
		Enabled:            c.enabled,
		DDProfilingEnabled: strings.EqualFold(strings.TrimSpace(os.Getenv("DD_PROFILING_ENABLED")), "true"),
	}
}

func (c *DatadogProfilerController) options() []profiler.Option {
	serviceName := strings.TrimSpace(c.cfg.ServiceName)
	if serviceName == "" {
		serviceName = "graphdb"
	}
	options := []profiler.Option{profiler.WithService(serviceName)}
	if environment := strings.TrimSpace(c.cfg.Environment); environment != "" {
		options = append(options, profiler.WithEnv(environment))
	}
	if version := strings.TrimSpace(c.cfg.Version); version != "" {
		options = append(options, profiler.WithVersion(version))
	}
	return options
}
