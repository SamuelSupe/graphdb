package observability

import (
	"errors"
	"os"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/profiler"
)

func TestDatadogProfilerControllerEnableAndDisable(t *testing.T) {
	t.Setenv("DD_PROFILING_ENABLED", "false")
	starts := 0
	stops := 0
	controller := newDatadogProfilerController(DatadogProfilerConfig{}, func(...profiler.Option) error {
		starts++
		return nil
	}, func() {
		stops++
	})

	status, err := controller.SetEnabled(true)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !status.Enabled || !status.DDProfilingEnabled || starts != 1 || os.Getenv("DD_PROFILING_ENABLED") != "true" {
		t.Fatalf("enable status=%#v starts=%d env=%q", status, starts, os.Getenv("DD_PROFILING_ENABLED"))
	}

	status, err = controller.SetEnabled(false)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if status.Enabled || status.DDProfilingEnabled || stops != 1 || os.Getenv("DD_PROFILING_ENABLED") != "false" {
		t.Fatalf("disable status=%#v stops=%d env=%q", status, stops, os.Getenv("DD_PROFILING_ENABLED"))
	}
}

func TestDatadogProfilerControllerRestoresEnvironmentAfterStartFailure(t *testing.T) {
	t.Setenv("DD_PROFILING_ENABLED", "false")
	controller := newDatadogProfilerController(DatadogProfilerConfig{}, func(...profiler.Option) error {
		return errors.New("agent unavailable")
	}, func() {})

	status, err := controller.SetEnabled(true)
	if err == nil {
		t.Fatal("enable error = nil, want error")
	}
	if status.Enabled || status.DDProfilingEnabled || os.Getenv("DD_PROFILING_ENABLED") != "false" {
		t.Fatalf("failure status=%#v env=%q", status, os.Getenv("DD_PROFILING_ENABLED"))
	}
}
