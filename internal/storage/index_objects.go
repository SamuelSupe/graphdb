package storage

func firstIndexObjectKey(objects []IndexObject, role string, fallback string) string {
	object, ok := firstIndexObject(objects, role, IndexObject{Key: fallback})
	if !ok {
		return fallback
	}
	return object.Key
}

func firstIndexObject(objects []IndexObject, role string, fallback IndexObject) (IndexObject, bool) {
	for _, object := range objects {
		if object.Role == role && object.Key != "" {
			return object, true
		}
	}
	for _, object := range objects {
		if object.Key != "" {
			return object, true
		}
	}
	return fallback, fallback.Key != ""
}
