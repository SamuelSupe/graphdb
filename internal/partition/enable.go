package partition

type EnableRequest struct {
	Tenant          string           `json:"tenant_id"`
	Epoch           uint64           `json:"epoch"`
	Groups          map[string]Group `json:"groups"`
	Attempt         uint64           `json:"attempt,omitempty"`
	GenerationFloor int64            `json:"generation_floor,omitempty"`
}

type ConversionCancellation struct {
	Attempt    uint64 `json:"attempt"`
	Generation int64  `json:"generation"`
	Activated  bool   `json:"activated,omitempty"`
}

func (g Graph) ConvertsSource(generation int64) bool {
	if g.ConversionAttempt != 0 {
		return g.ConversionSource == generation
	}
	return g.Generation == generation+1
}
