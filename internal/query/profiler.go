package query

import "time"

type profiler struct {
	enabled bool
	ops     []OperatorStat
}

func newProfiler(enabled bool) *profiler {
	return &profiler{enabled: enabled}
}

func (p *profiler) measure(name string, detail string, cost int, fn func() (int, error)) error {
	if p == nil || !p.enabled {
		_, err := fn()
		return err
	}
	start := time.Now()
	rows, err := fn()
	p.ops = append(p.ops, OperatorStat{
		Name:       name,
		Detail:     detail,
		Rows:       rows,
		Cost:       cost,
		DurationMS: float64(time.Since(start).Microseconds()) / 1000,
	})
	return err
}

func (p *profiler) snapshot() []OperatorStat {
	if p == nil || !p.enabled || len(p.ops) == 0 {
		return nil
	}
	out := make([]OperatorStat, len(p.ops))
	copy(out, p.ops)
	return out
}
