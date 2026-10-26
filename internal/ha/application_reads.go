package ha

import (
	"context"
	"time"
)

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
	a.readers.RLock()
	a.readMu.Lock()
	if a.activeReads == nil {
		a.activeReads = make(map[*applicationRead]struct{})
	}
	a.activeReads[read] = struct{}{}
	a.readMu.Unlock()
	return ctx, func() {
		cancel()
		a.readMu.Lock()
		delete(a.activeReads, read)
		a.readMu.Unlock()
		a.readers.RUnlock()
	}
}
func (a *Application) lockReaders() {
	if a.readers.TryLock() {
		return
	}
	// Drain ordinary reads before revoking a slow or blocked network peer.
	// The pending writer gates new reads. Leave readMu available for completed
	// handlers to unregister before net/http flushes their buffered responses.
	interrupted := make(chan struct{})
	timer := time.AfterFunc(time.Second, func() {
		a.readMu.Lock()
		for read := range a.activeReads {
			read.cancel()
			read.interrupt()
		}
		a.readMu.Unlock()
		close(interrupted)
	})
	a.readers.Lock()
	if !timer.Stop() {
		<-interrupted
	}
}
