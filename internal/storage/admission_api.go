package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/admission"

type (
	BackpressureConfig   = admission.BackpressureConfig
	BackpressureError    = admission.BackpressureError
	BackpressureObserver = admission.BackpressureObserver
	BackpressureReason   = admission.BackpressureReason
	WritePressure        = admission.WritePressure
)

var (
	ErrBackpressure           = admission.ErrBackpressure
	ErrObjectStoreUnavailable = admission.ErrObjectStoreUnavailable
)

func NewWritePressure(config BackpressureConfig) *WritePressure {
	return admission.NewWritePressure(config)
}
