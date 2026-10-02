package observability

import (
	"io"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
)

var processStarted = time.Now()

func WriteRuntimeMetrics(w io.Writer) {
	build := buildinfo.Current()
	WriteInfo(w, "graphdb_build_info", "Binary identity of the process serving this endpoint.", []string{"version", "commit", "go_version"}, []string{build.Version, build.Commit, build.GoVersion})
	WriteScalar(w, "graphdb_process_start_time_seconds", "Process instrumentation start time as Unix seconds.", "gauge", float64(processStarted.UnixNano())/1e9)
	WriteScalar(w, "graphdb_go_goroutines", "Current Go goroutines.", "gauge", float64(runtime.NumGoroutine()))
	definitions := []struct{ source, name, help, kind string }{
		{"/memory/classes/heap/objects:bytes", "graphdb_go_heap_objects_bytes", "Bytes occupied by live or unswept heap objects.", "gauge"},
		{"/memory/classes/total:bytes", "graphdb_go_memory_reserved_bytes", "Memory mapped by the Go runtime; not process RSS.", "gauge"},
		{"/gc/heap/objects:objects", "graphdb_go_heap_objects", "Live or unswept heap object count.", "gauge"},
		{"/gc/heap/allocs:bytes", "graphdb_go_heap_allocated_bytes_total", "Cumulative bytes allocated on the Go heap.", "counter"},
		{"/gc/cycles/total:gc-cycles", "graphdb_go_gc_cycles_total", "Completed garbage collection cycles.", "counter"},
		{"/cpu/classes/gc/total:cpu-seconds", "graphdb_go_gc_cpu_seconds_total", "Estimated cumulative CPU seconds spent in garbage collection.", "counter"},
	}
	samples := make([]metrics.Sample, len(definitions))
	for i, definition := range definitions {
		samples[i].Name = definition.source
	}
	metrics.Read(samples)
	for i, sample := range samples {
		var value float64
		switch sample.Value.Kind() {
		case metrics.KindUint64:
			value = float64(sample.Value.Uint64())
		case metrics.KindFloat64:
			value = sample.Value.Float64()
		default:
			continue
		}
		d := definitions[i]
		WriteScalar(w, d.name, d.help, d.kind, value)
	}
}
