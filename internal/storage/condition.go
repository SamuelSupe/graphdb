package storage

func checkCondition(condition PutCondition, currentETag string, exists bool) error {
	if condition.IfNoneMatch && exists {
		return ErrConflict
	}
	if condition.IfMatch != "" {
		if !exists || currentETag != condition.IfMatch {
			return ErrConflict
		}
	}
	return nil
}
