package observability

import (
	"fmt"
	"testing"
	"time"
)

func TestTenantMetricCardinalityIsBoundedWithoutLosingCounts(t *testing.T) {
	m := NewMetrics()
	for i := range 2500 {
		m.RecordQuery(fmt.Sprintf("tenant-%d", i), "match", "error", time.Millisecond, false)
	}
	if len(m.queries) > 1025 {
		t.Fatalf("unbounded tenant series: %d", len(m.queries))
	}
	var count float64
	for _, n := range m.queries {
		count += n
	}
	if count != 2500 {
		t.Fatalf("lost operations during aggregation: %g", count)
	}
}
