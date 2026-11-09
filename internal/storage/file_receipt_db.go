package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	}
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %s: %v", ErrReceiptStoreUnavailable, key, value)
			s.receiptMu.Lock()
			h.err = err
			s.receiptMu.Unlock()
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	return operation(h.db)
}

func (s *FileStore) acquireReceiptDB(ctx context.Context, key string, write bool) (*receiptDBHandle, error) {
	unlock, err := s.lockObjectContext(ctx, key)
	if err != nil {
		return nil, err
	}
	defer unlock()
	s.receiptMu.Lock()
	if h := s.receiptDBs[key]; h != nil {
		err := h.err
		if err == nil {
			if h.users == 0 {
				h.idle = make(chan struct{})
			}
			h.users++
		}
		s.receiptMu.Unlock()
		return h, err
	}
	s.receiptMu.Unlock()

	h, err := s.openReceiptDB(key, write)
	if err != nil && h.err == nil {
		return nil, err
	}
	s.receiptMu.Lock()
	defer s.receiptMu.Unlock()
	if s.receiptDBs == nil {
		s.receiptDBs = make(map[string]*receiptDBHandle)
	}
	if err == nil {
		h.writer = semaphore.NewWeighted(1)
		h.users = 1
		h.idle = make(chan struct{})
	}
	s.receiptDBs[key] = h
	s.trimReceiptDBs()
	return h, err
}

func (s *FileStore) openReceiptDB(key string, create bool) (h *receiptDBHandle, err error) {
	h = &receiptDBHandle{}
	quarantine := false
	defer func() {
		if value := recover(); value != nil {
			// Open can panic before it returns the DB handle. Release the file
			// lock directly and quarantine the path until restart or replacement.
			if h.file != nil {
				_ = closePoisonedReceiptFile(h.file)
				h.file = nil
			}
			err = fmt.Errorf("open bbolt: %v", value)
			quarantine = true
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			err = fmt.Errorf("%w: %s: %w", ErrReceiptStoreUnavailable, key, err)
			if quarantine {
				h.err = err
			}
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))

	filename, err := s.path(key)
	if err != nil {
		return h, err
	}
	if create {
		err = s.ensureSafeParent(filename, nil)
	} else {
		err = s.verifySafeParent(filename)
	}
	if err != nil {
		return h, err
	}
	info, err := os.Lstat(filename)
	if os.IsNotExist(err) {
		if !create {
			return h, ErrNotFound
		}
	} else if err != nil {
		return h, err
	} else if !info.Mode().IsRegular() || info.Size() == 0 {
		quarantine = true
		return h, fmt.Errorf("receipt database is not a nonempty regular file")
	}
	options := &bolt.Options{Timeout: time.Second, OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
		if info != nil {
			flags &^= os.O_CREATE
		}
		file, err := os.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, mode)
		h.file = file
		return file, err
	}}
	h.db, err = bolt.Open(filename, 0600, options)
	if err != nil {
		if info == nil && h.file != nil {
			// Failed initialization cannot have published any receipt.
			err = errors.Join(err, os.Remove(filename))
		} else {
			quarantine = errors.Is(err, bolt.ErrInvalid) || errors.Is(err, bolt.ErrChecksum) || errors.Is(err, bolt.ErrVersionMismatch)
		}
		return h, err
	}
	// Establish the directory barrier before letting a transaction publish
	// a receipt or maintenance remove its original loose copy.
	if err := syncDir(filepath.Dir(filename)); err != nil {
		_ = h.db.Close()
		h.db, h.file = nil, nil
		return h, err
	}
	return h, nil
}

func (s *FileStore) releaseReceiptDB(h *receiptDBHandle) {
	s.receiptMu.Lock()
	defer s.receiptMu.Unlock()
	h.users--
	if h.users == 0 {
		close(h.idle)
		h.lastUsed = time.Now()
	}
	s.trimReceiptDBs()
}

func (s *FileStore) trimReceiptDBs() {
	if len(s.receiptDBs) <= maxOpenReceiptDBs {
		return
	}
	var oldestKey string
	var oldest *receiptDBHandle
	open := 0
	for key, h := range s.receiptDBs {
		if h.db != nil {
			open++
		}
		if h.users == 0 && h.err == nil && (oldest == nil || h.lastUsed.Before(oldest.lastUsed)) {
			oldestKey, oldest = key, h
		}
	}
	if open > maxOpenReceiptDBs && oldest != nil {
		_ = oldest.close()
		delete(s.receiptDBs, oldestKey)
	}
}

// The caller holds the physical object's lock, preventing new handle users
// from racing a direct file replacement or unlink.
func (s *FileStore) closeReceiptDB(ctx context.Context, key string) error {
	s.receiptMu.Lock()
	h := s.receiptDBs[key]
	if h == nil {
		s.receiptMu.Unlock()
		return nil
	}
	idle := h.idle
	s.receiptMu.Unlock()
	if idle != nil {
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.receiptMu.Lock()
	defer s.receiptMu.Unlock()
	if s.receiptDBs[key] == h {
		delete(s.receiptDBs, key)
		return h.close()
	}
	return nil
}

// All IO permits are drained during directory replacement or store shutdown.
func (s *FileStore) closeReceiptDBs(prefix string) error {
	s.receiptMu.Lock()
	defer s.receiptMu.Unlock()
	var err error
	for key, h := range s.receiptDBs {
		if strings.HasPrefix(key, prefix) {
			err = errors.Join(err, h.close())
			delete(s.receiptDBs, key)
		}
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
