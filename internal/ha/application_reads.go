package ha

import "context"

type applicationRead struct {
	cancel    context.CancelFunc
	interrupt func()
}

// Immutable publications run beside reads. Operations that replace or reclaim
// files revoke reads before taking the application barrier; a slow network peer
// must not retain that barrier indefinitely.
func (a *Application) readContext(parent context.Context, interrupt func()) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	read := &applicationRead{cancel: cancel, interrupt: interrupt}
	a.readMu.Lock()
	a.readers.RLock()
	if a.activeReads == nil {
		a.activeReads = make(map[*applicationRead]struct{})
	}
	a.activeReads[read] = struct{}{}
	a.readMu.Unlock()
	return ctx, func() {
		cancel()
		// Release the barrier before removing the registration: lockReaders may
		// hold readMu while waiting for every handler to observe cancellation.
		a.readers.RUnlock()
		a.readMu.Lock()
		delete(a.activeReads, read)
		a.readMu.Unlock()
	}
}
func (a *Application) lockReaders() {
	a.readMu.Lock()
	for read := range a.activeReads {
		read.cancel()
		read.interrupt()
	}
	a.readers.Lock()
	a.readMu.Unlock()
}
