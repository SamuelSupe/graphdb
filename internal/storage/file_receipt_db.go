package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/semaphore"
)

var ErrReceiptStoreUnavailable = errors.New("tenant receipt database unavailable")

const maxOpenReceiptDBs = 64

type receiptDBHandle struct {
	db       *bolt.DB
	file     *os.File
	writer   *semaphore.Weighted
	err      error
	users    int
	idle     chan struct{}
	closing  chan struct{}
	lastUsed time.Time
}

func (s *FileStore) isReceiptDB(key string) bool {
	relative, ok := strings.CutPrefix(key, s.receiptPrefix)
	_, name, found := strings.Cut(relative, "/")
	return s.receiptPrefix != "" && ok && found && name == receiptDatabase
}

// Logical receipt operations hold a directory IO permit and pin the handle,
// so a tenant rename, purge or shutdown cannot leave a writer on an old inode.
func (s *FileStore) withReceiptDB(ctx context.Context, key string, write bool, operation func(*bolt.DB) error) (err error) {
	if write && s.receiptReadOnly {
		return fmt.Errorf("packed receipts are read-only for this command")
	}
	h, err := s.acquireReceiptDB(ctx, key, write)
	if err != nil {
		return err
	}
	defer s.releaseReceiptDB(h)
	if write {
		if err := h.writer.Acquire(ctx, 1); err != nil {
			return err
		}
		defer h.writer.Release(1)
		// A previous writer may have panicked while holding a bbolt lock.
		s.receiptMu.Lock()
		poison := h.err
		s.receiptMu.Unlock()
		if poison != nil {
			return poison
		}
		// A published graph can reuse objects renamed by another file batch.
		// Before a durable receipt can acknowledge it, drain that batch too.
		if err := s.syncPendingDirectories(); err != nil {
			return err
		}
	}
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("receipt transaction panic: %v", value)
			if receiptDatabasePanic(value) {
				err = fmt.Errorf("%w: %s: %w", ErrReceiptStoreUnavailable, key, err)
				s.receiptMu.Lock()
				h.err = err
				s.receiptMu.Unlock()
			}
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	return operation(h.db)
}

// bbolt reports page assertions as ordinary panics. Check the panic origin
// as well as mapped-memory faults, so a caller's own bug cannot poison a pack.
func receiptDatabasePanic(value any) bool {
	if _, ok := value.(interface {
		runtime.Error
		Addr() uintptr
	}); ok {
		return true
	}
	var pcs [32]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	panicking := false
	for {
		frame, more := frames.Next()
		if frame.Function == "runtime.gopanic" {
			panicking = true
		} else if panicking && !strings.HasPrefix(frame.Function, "runtime.") {
			return strings.HasPrefix(frame.Function, "go.etcd.io/bbolt.") || strings.HasPrefix(frame.Function, "go.etcd.io/bbolt/")
		}
		if !more {
			return false
		}
	}
}

func (s *FileStore) acquireReceiptDB(ctx context.Context, key string, write bool) (*receiptDBHandle, error) {
	unlock, err := s.lockObjectContext(ctx, key)
	if err != nil {
		return nil, err
	}
	defer unlock()
	for {
		s.receiptMu.Lock()
		h := s.receiptDBs[key]
		if h == nil {
			s.receiptMu.Unlock()
			break
		}
		if h.closing != nil {
			closing := h.closing
			s.receiptMu.Unlock()
			select {
			case <-closing:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		err := h.err
		if err == nil {
			if write && h.db.IsReadOnly() {
				s.receiptMu.Unlock()
				if err := s.closeReceiptDB(ctx, key); err != nil {
					return nil, err
				}
				continue
			}
			if h.users == 0 {
				h.idle = make(chan struct{})
			}
			h.users++
		}
		s.receiptMu.Unlock()
		return h, err
	}

	h, err := s.openReceiptDB(key, write)
	if err != nil && h.err == nil {
		return nil, err
	}
	s.receiptMu.Lock()
	if s.receiptDBs == nil {
		s.receiptDBs = make(map[string]*receiptDBHandle)
	}
	if err == nil {
		h.writer = semaphore.NewWeighted(1)
		h.users = 1
		h.idle = make(chan struct{})
	}
	s.receiptDBs[key] = h
	s.receiptMu.Unlock()
	s.trimReceiptDBs()
	return h, err
}

func (s *FileStore) openReceiptDB(key string, write bool) (*receiptDBHandle, error) {
	h := &receiptDBHandle{}
	filename, err := s.path(key)
	if err != nil {
		return h, err
	}
	if write {
		err = s.ensureSafeParent(filename, nil)
	} else {
		err = s.verifySafeParent(filename)
	}
	if err != nil {
		return h, err
	}
	info, err := os.Lstat(filename)
	switch {
	case os.IsNotExist(err):
		if !write {
			return h, ErrNotFound
		}
		if err := installEmptyReceiptDB(filename); err != nil {
			return h, err
		}
	case err != nil:
		return h, err
	case !info.Mode().IsRegular() || info.Size() == 0:
		// A zero-length canonical file might be a previously committed
		// database that was truncated. Never turn that into a missing receipt.
		h.err = fmt.Errorf("%w: %s is not a nonempty regular file", ErrReceiptStoreUnavailable, key)
		return h, h.err
	}

	h, err = openBoltReceipt(filename, !write, false)
	if errors.Is(err, ErrReceiptStoreUnavailable) || errors.Is(err, bolt.ErrInvalid) ||
		errors.Is(err, bolt.ErrChecksum) || errors.Is(err, bolt.ErrVersionMismatch) {
		h.err = fmt.Errorf("%w: %s: %w", ErrReceiptStoreUnavailable, key, err)
		return h, h.err
	}
	if err != nil {
		return h, err
	}
	if write {
		// A failed barrier after install must be retried before any write.
		if err := syncDir(filepath.Dir(filename)); err != nil {
			return &receiptDBHandle{}, errors.Join(err, h.close())
		}
	}
	return h, nil
}

func installEmptyReceiptDB(filename string) error {
	temp, err := os.CreateTemp(filepath.Dir(filename), ".tmp-receipts-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Close(); err != nil {
		return err
	}
	h, err := openBoltReceipt(name, false, false)
	if err != nil {
		return err
	}
	if err := h.close(); err != nil {
		return err
	}
	// bbolt has synced the initialization pages. The canonical name
	// never refers to an incomplete database, even if creation is interrupted.
	return os.Rename(name, filename)
}

func openBoltReceipt(filename string, readOnly, preloadFreelist bool) (h *receiptDBHandle, err error) {
	h = &receiptDBHandle{}
	defer func() {
		if value := recover(); value != nil {
			if h.file != nil {
				_ = closePoisonedReceiptFile(h.file)
				h.file = nil
			}
			err = fmt.Errorf("open bbolt: %v", value)
			if receiptDatabasePanic(value) {
				err = fmt.Errorf("%w: %w", ErrReceiptStoreUnavailable, err)
			}
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	h.db, err = bolt.Open(filename, 0600, &bolt.Options{
		ReadOnly: readOnly, PreLoadFreelist: preloadFreelist, Timeout: time.Second,
		OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
			file, err := os.OpenFile(name, flags&^os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, mode)
			h.file = file
			return file, err
		},
	})
	return h, err
}

func (s *FileStore) releaseReceiptDB(h *receiptDBHandle) {
	s.receiptMu.Lock()
	h.users--
	if h.users == 0 {
		close(h.idle)
		h.lastUsed = time.Now()
	}
	s.receiptMu.Unlock()
	s.trimReceiptDBs()
}

func (s *FileStore) trimReceiptDBs() {
	s.receiptMu.Lock()
	if len(s.receiptDBs) <= maxOpenReceiptDBs {
		s.receiptMu.Unlock()
		return
	}
	var oldestKey string
	var oldest *receiptDBHandle
	open := 0
	for key, h := range s.receiptDBs {
		if h.db != nil && h.closing == nil {
			open++
		}
		if h.users == 0 && h.err == nil && h.closing == nil && (oldest == nil || h.lastUsed.Before(oldest.lastUsed)) {
			oldestKey, oldest = key, h
		}
	}
	if open <= maxOpenReceiptDBs || oldest == nil {
		s.receiptMu.Unlock()
		return
	}
	// Keep a tombstone until flock is released. A concurrent open of this
	// tenant waits without holding the cache lock or delaying other tenants.
	oldest.closing = make(chan struct{})
	s.receiptMu.Unlock()
	_ = oldest.close()
	s.receiptMu.Lock()
	delete(s.receiptDBs, oldestKey)
	close(oldest.closing)
	s.receiptMu.Unlock()
}

// The caller holds the physical object's lock, preventing new handle users
// from racing a direct file replacement, unlink or read-only handle upgrade.
func (s *FileStore) closeReceiptDB(ctx context.Context, key string) error {
	for {
		s.receiptMu.Lock()
		h := s.receiptDBs[key]
		if h == nil {
			s.receiptMu.Unlock()
			return nil
		}
		wait := h.closing
		if wait == nil && h.users == 0 {
			delete(s.receiptDBs, key)
			s.receiptMu.Unlock()
			return h.close()
		}
		if wait == nil {
			wait = h.idle
		}
		s.receiptMu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// All IO permits are drained during directory replacement or store shutdown.
func (s *FileStore) closeReceiptDBs(prefix string) error {
	s.receiptMu.Lock()
	var handles []*receiptDBHandle
	for key, h := range s.receiptDBs {
		if strings.HasPrefix(key, prefix) {
			handles = append(handles, h)
			delete(s.receiptDBs, key)
		}
	}
	s.receiptMu.Unlock()
	var err error
	for _, h := range handles {
		err = errors.Join(err, h.close())
	}
	return err
}

func (h *receiptDBHandle) close() error {
	if h.db == nil {
		return nil
	}
	if h.err != nil {
		// A panic in bbolt may leave internal mutexes held. Never reenter
		// that handle; close its descriptor and leave the mapping to exit.
		return closePoisonedReceiptFile(h.file)
	}
	return h.db.Close()
}

func closePoisonedReceiptFile(file *os.File) error {
	// A retained mapping can keep the kernel file reference alive after
	// Close. Release flock explicitly so offline verification can reopen it.
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}
