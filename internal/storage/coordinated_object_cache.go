package storage

func (s *TenantStore) clearWriterObjectKey(key string) {
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.invalidateKey(key)
	}
}

func (s *TenantStore) clearCoordinatedWriterObjectKey(key string) {
	if !s.coordinated() {
		return
	}
	s.clearWriterObjectKey(key)
}
