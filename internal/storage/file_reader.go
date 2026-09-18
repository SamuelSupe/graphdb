package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/apache/arrow-go/v18/parquet"
)

type fileReader interface {
	parquet.ReaderAtSeeker
	io.Closer
}

type fileOpener interface {
	OpenReader(context.Context, string) (fileReader, error)
}

type managedFileReader struct {
	parquet.ReaderAtSeeker
	ctx   context.Context
	close func() error
	once  sync.Once
	err   error
}

func (r *managedFileReader) ReadAt(p []byte, off int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.ReaderAtSeeker.ReadAt(p, off)
}
func (r *managedFileReader) Close() error {
	r.once.Do(func() { r.err = r.close() })
	return r.err
}

func openFileReader(ctx context.Context, objects ObjectStore, key string) (fileReader, error) {
	if opener, ok := objects.(fileOpener); ok {
		return opener.OpenReader(ctx, key)
	}
	data, err := objects.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &managedFileReader{ReaderAtSeeker: bytes.NewReader(data), ctx: ctx, close: func() error { return nil }}, nil
}

func (s *FileStore) OpenReader(ctx context.Context, key string) (fileReader, error) {
	releaseOperation, operationErr := s.beginLifecycleOperation(ctx)
	if operationErr != nil {
		return nil, operationErr
	}
	transferred := false
	defer func() {
		if !transferred {
			releaseOperation()
		}
	}()
	unlock, err := s.lockDirectoryIO(ctx)
	if err != nil {
		return nil, err
	}
	// An open descriptor remains valid across directory renames. Only opening
	// it participates in the switch barrier; its lifetime still pins Close.
	defer unlock()
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	if err := s.verifySafeParent(path); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), key)
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, ErrNotFound
	}
	transferred = true
	return &managedFileReader{ReaderAtSeeker: f, ctx: ctx, close: func() error { defer releaseOperation(); return f.Close() }}, nil
}

func (s *MeteredObjectStore) UnwrapObjectStore() ObjectStore       { return s.Inner }
func (s *ReadProtectedObjectStore) UnwrapObjectStore() ObjectStore { return s.Inner }
func (s *DelayedReadObjectStore) UnwrapObjectStore() ObjectStore   { return s.Inner }

func (s *WriterObjectCache) OpenReader(ctx context.Context, key string) (fileReader, error) {
	return openFileReader(ctx, s.Inner, key)
}
func (s *DelayedReadObjectStore) OpenReader(ctx context.Context, key string) (fileReader, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return openFileReader(ctx, s.Inner, key)
}
func (s *ReadProtectedObjectStore) OpenReader(ctx context.Context, key string) (fileReader, error) {
	release, err := s.acquireRead(ctx)
	if err != nil {
		return nil, err
	}
	r, err := openFileReader(ctx, s.Inner, key)
	if err != nil {
		release()
		return nil, err
	}
	return &managedFileReader{ReaderAtSeeker: r, ctx: ctx, close: func() error { defer release(); return r.Close() }}, nil
}
func (s *MeteredObjectStore) OpenReader(ctx context.Context, key string) (fileReader, error) {
	ctx, done := s.start(ctx, "read_file", key, -1)
	r, err := openFileReader(ctx, s.Inner, key)
	if err != nil {
		done(err)
		return nil, err
	}
	return &managedFileReader{ReaderAtSeeker: r, ctx: ctx, close: func() error { err := r.Close(); done(err); return err }}, nil
}
