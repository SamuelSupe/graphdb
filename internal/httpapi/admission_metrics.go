package httpapi

import (
	"fmt"
	"io"

	"github.com/SamuelSupe/graphdb/v2/internal/observability"
)

type admissionObservation struct {
	Enabled        bool    `json:"enabled"`
	Active         int64   `json:"active"`
	Waiting        int64   `json:"waiting"`
	GlobalLimit    int     `json:"global_limit"`
	PerTenantLimit int     `json:"per_tenant_limit"`
	QueueSeconds   float64 `json:"queue_timeout_seconds"`
}

func (a *QueryAdmission) observation() admissionObservation {
	if a == nil {
		return admissionObservation{}
	}
	return admissionObservation{Enabled: true, Active: a.active.Load(), Waiting: a.waiting.Load(), GlobalLimit: cap(a.global), PerTenantLimit: a.perTenantMax, QueueSeconds: a.queueTimeout.Seconds()}
}

func (a *WriteAdmission) observation() admissionObservation {
	if a == nil {
		return admissionObservation{}
	}
	return admissionObservation{Enabled: true, Active: a.active.Load(), Waiting: a.waiting.Load(), GlobalLimit: cap(a.global), PerTenantLimit: a.perTenantMax, QueueSeconds: a.queueTimeout.Seconds()}
}

func (s *Server) admissionObservations() map[string]admissionObservation {
	return map[string]admissionObservation{"query": s.Admission.observation(), "read": s.ReadAdmission.observation(), "write": s.WriteAdmission.observation()}
}

func (s *Server) writeAdmissionMetrics(w io.Writer) {
	observations := s.admissionObservations()
	for _, metric := range []struct {
		name, help string
		value      func(admissionObservation) float64
	}{
		{"enabled", "Local admission controller is configured.", func(a admissionObservation) float64 {
			if a.Enabled {
				return 1
			}
			return 0
		}},
		{"active", "Requests currently holding local admission permits.", func(a admissionObservation) float64 { return float64(a.Active) }},
		{"waiting", "Requests currently acquiring local tenant or global admission permits.", func(a admissionObservation) float64 { return float64(a.Waiting) }},
		{"global_limit", "Local global concurrency limit, zero means unlimited.", func(a admissionObservation) float64 { return float64(a.GlobalLimit) }},
		{"per_tenant_limit", "Local per-tenant concurrency limit, zero means unlimited.", func(a admissionObservation) float64 { return float64(a.PerTenantLimit) }},
		{"queue_timeout_seconds", "Configured local admission queue timeout, zero means request context only.", func(a admissionObservation) float64 { return a.QueueSeconds }},
	} {
		name := "graphdb_admission_" + metric.name
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, metric.help, name)
		for _, pool := range []string{"query", "read", "write"} {
			fmt.Fprintf(w, "%s{pool=%q} %g\n", name, pool, metric.value(observations[pool]))
		}
	}
	observability.WriteScalar(w, "graphdb_queries_running", "Queries registered in this local process, including admission waiters.", "gauge", float64(s.QueryRegistry.Count()))
}
