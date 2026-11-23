package ha

import (
	"errors"
	"net/http"
	"sync"
	"time"
)

// Give revoked reads time to send a small error response, and still bound
// how long a blocked response can retain the application read barrier.
const interruptedReadWriteGrace = time.Second

var errRaftReadInterrupted = errors.New("Raft maintenance interrupted the read")

type raftReadResponse struct {
	http.ResponseWriter
	cluster *Cluster
	status  int

	mu          sync.Mutex
	started     bool
	interrupted bool
	finished    bool
	deadline    time.Time
	ndjson      bool
}

func (w *raftReadResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *raftReadResponse) interrupt() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.interrupted || w.finished {
		return
	}
	w.interrupted = true
	if w.started {
		w.limitWriteTime()
	}
}

func (w *raftReadResponse) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.interrupted {
		return errRaftReadInterrupted
	}
	err := http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
	if err == nil {
		w.deadline = deadline
	}
	return err
}

func (w *raftReadResponse) WriteHeader(status int) {
	if w.started || w.status != 0 {
		return
	}
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	// A final status can still be replaced until the first body write or flush.
	w.status = status
}

func (w *raftReadResponse) Write(data []byte) (int, error) {
	if err := w.start(); err != nil {
		return 0, err
	}
	return w.ResponseWriter.Write(data)
}

func (w *raftReadResponse) FlushError() error {
	if err := w.start(); err != nil {
		return err
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *raftReadResponse) Flush() { _ = w.FlushError() }

func (w *raftReadResponse) start() error {
	w.mu.Lock()
	started, interrupted := w.started, w.interrupted
	w.started = true
	if !started && interrupted {
		w.limitWriteTime()
	}
	w.mu.Unlock()
	if interrupted {
		if !started {
			// Discard headers that described the handler's original body.
			for _, name := range []string{"Content-Length", "Content-Encoding", "Content-Range", "Content-Disposition", "Transfer-Encoding", "Trailer"} {
				w.Header().Del(name)
			}
			w.cluster.writeError(w.ResponseWriter, errRaftReadInterrupted)
		}
		return errRaftReadInterrupted
	}
	if !started {
		w.ndjson = w.Header().Get("Content-Type") == "application/x-ndjson"
		if w.status != 0 {
			w.ResponseWriter.WriteHeader(w.status)
		}
	}
	return nil
}

func (w *raftReadResponse) limitWriteTime() {
	deadline := time.Now().Add(interruptedReadWriteGrace)
	if w.deadline.IsZero() || deadline.Before(w.deadline) {
		_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
	}
}

func (w *raftReadResponse) finish() {
	w.mu.Lock()
	w.finished = true
	started, interrupted := w.started, w.interrupted
	w.mu.Unlock()
	if !started {
		_ = w.start()
	} else if interrupted && w.ndjson {
		// A normal HTTP EOF would let clients accept a stream missing its final record.
		panic(http.ErrAbortHandler)
	}
}
