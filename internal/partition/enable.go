package partition

type EnableRequest struct {
	Tenant string           `json:"tenant_id"`
	Epoch  uint64           `json:"epoch"`
	Groups map[string]Group `json:"groups"`
}
