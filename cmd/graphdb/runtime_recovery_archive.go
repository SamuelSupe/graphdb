package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
)

type runtimeArchiveContents struct {
	Usage map[string]int64
	Files []string
}

func inspectRuntimeArchive(input io.ReadSeeker, roots map[string]string, expected runtimeArchiveManifest, limit int64) (*runtimeArchiveContents, error) {
	if _, err := input.Seek(sha256.Size, io.SeekStart); err != nil {
		return nil, err
	}
	compressed, err := gzip.NewReader(input)
	if err != nil {
		return nil, err
	}
	defer compressed.Close()
	limited := &io.LimitedReader{R: compressed, N: limit + 1}
	archive := tar.NewReader(limited)
	header, err := archive.Next()
	if err != nil || header.Name != "manifest.json" || header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > 1<<20 {
		return nil, fmt.Errorf("invalid runtime archive manifest")
	}
	data, err := io.ReadAll(archive)
	if err != nil {
		return nil, err
	}
	var manifest runtimeArchiveManifest
	if json.Unmarshal(data, &manifest) != nil {
		return nil, fmt.Errorf("invalid runtime archive manifest")
	}
	actual, _ := json.Marshal(manifest)
	wanted, _ := json.Marshal(expected)
	if !bytes.Equal(actual, wanted) {
		return nil, fmt.Errorf("runtime archive deployment mode or Raft identity differs from the target")
	}
	seen := map[string]bool{}
	contents := &runtimeArchiveContents{Usage: map[string]int64{}}
	usage := contents.Usage
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		role, relative, err := runtimeArchiveName(header.Name, roots)
		if err != nil {
			return nil, err
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > limit || seen[header.Name] {
			return nil, fmt.Errorf("invalid or duplicate runtime archive object")
		}
		seen[header.Name] = true
		if !runtimeLockObject(role, relative) {
			contents.Files = append(contents.Files, header.Name)
		}
		if header.Size > limit-usage[role] {
			return nil, fmt.Errorf("runtime archive exceeds byte budget")
		}
		usage[role] += header.Size
		if _, err := io.Copy(io.Discard, archive); err != nil {
			return nil, err
		}
	}
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return nil, err
	}
	if limited.N == 0 {
		return nil, fmt.Errorf("runtime archive exceeds uncompressed byte budget")
	}
	return contents, nil
}

type runtimeBudgetWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *runtimeBudgetWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, fmt.Errorf("runtime archive exceeds uncompressed byte budget")
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}
