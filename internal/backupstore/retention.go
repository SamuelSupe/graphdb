package backupstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var ErrBackupInUse = errors.New("backup is in use")

// Pin protects manifest lookup and the subsequent download as one read. Retention
// never waits for network readers, and retries skipped backups on the next run.
func (r *Repository) Pin(uri string) (func(), error) {
	if _, _, err := r.ParseURI(uri); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readers == nil {
		r.readers = make(map[string]int)
	}
	if r.readers[uri] < 0 {
		return nil, ErrBackupInUse
	}
	r.readers[uri]++
	return sync.OnceFunc(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.readers[uri]--
		if r.readers[uri] == 0 {
			delete(r.readers, uri)
		}
	}), nil
}

func (r *Repository) Verify(ctx context.Context, uri string) (Manifest, error) {
	release, err := r.Pin(uri)
	if err != nil {
		return Manifest{}, err
	}
	defer release()
	m, err := r.ReadManifest(ctx, uri)
	if err != nil {
		return Manifest{}, err
	}
	return m, r.Download(ctx, m, io.Discard)
}

// DeleteAutomatic removes visibility before payload deletion. The caller must
// durably checkpoint entry before calling, so an interrupted deletion can resume
// even after its manifest disappeared. Manual backups are never eligible.
func (r *Repository) DeleteAutomatic(ctx context.Context, entry Entry) error {
	tenant, id, err := r.ParseURI(entry.BackupKey)
	if err != nil {
		return err
	}
	if err := r.validate(entry.Manifest, tenant, id); err != nil {
		return err
	}
	if !entry.Automatic {
		return fmt.Errorf("retention cannot delete a manual backup")
	}
	r.mu.Lock()
	if r.readers == nil {
		r.readers = make(map[string]int)
	}
	if r.readers[entry.BackupKey] != 0 {
		r.mu.Unlock()
		return ErrBackupInUse
	}
	r.readers[entry.BackupKey] = -1
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.readers, entry.BackupKey); r.mu.Unlock() }()
	current, err := r.ReadManifest(ctx, entry.BackupKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil && current != entry.Manifest {
		return fmt.Errorf("backup changed before retention")
	}
	if _, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(r.manifestKey(tenant, id))}); err != nil {
		return err
	}
	_, err = r.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(entry.SnapshotKey)})
	return err
}
