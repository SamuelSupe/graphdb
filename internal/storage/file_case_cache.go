package storage

import (
	"os"
	"strings"
	"unicode"
)

type directorySpellings struct {
	info  os.FileInfo
	names map[string]string
	bytes int
}

// EqualFold includes equivalences (such as sigma) that lowercase alone misses.
func directoryFold(name string) string {
	return strings.Map(func(r rune) rune {
		smallest := r
		for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
			if next < smallest {
				smallest = next
			}
		}
		return smallest
	}, name)
}
func (s *FileStore) checkDirectorySpelling(parent, part string) error {
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errFileStoreKey
	}
	s.caseMu.Lock()
	cached, ok := s.directorySpellings[parent]
	s.caseMu.Unlock()
	if !ok || !os.SameFile(cached.info, info) || !cached.info.ModTime().Equal(info.ModTime()) || cached.info.Size() != info.Size() {
		entries, err := os.ReadDir(parent)
		if err != nil {
			return err
		}
		cached = directorySpellings{info: info, names: make(map[string]string, len(entries))}
		for _, entry := range entries {
			name := entry.Name()
			fold := directoryFold(name)
			if previous, exists := cached.names[fold]; exists && previous != name {
				cached.names[fold] = ""
			} else {
				cached.names[fold] = name
			}
			cached.bytes += len(name) + len(fold) + 32
		}
		after, err := os.Lstat(parent)
		if err != nil {
			return err
		}
		if os.SameFile(info, after) && info.ModTime().Equal(after.ModTime()) && info.Size() == after.Size() && cached.bytes <= 8<<20 {
			s.caseMu.Lock()
			if previous, exists := s.directorySpellings[parent]; exists {
				s.directorySpellingBytes -= previous.bytes
				delete(s.directorySpellings, parent)
			}
			if len(s.directorySpellings) >= 256 || s.directorySpellingBytes+cached.bytes > 8<<20 {
				s.directorySpellings = nil
				s.directorySpellingBytes = 0
			}
			if s.directorySpellings == nil {
				s.directorySpellings = map[string]directorySpellings{}
			}
			s.directorySpellings[parent] = cached
			s.directorySpellingBytes += cached.bytes
			s.caseMu.Unlock()
		}
	}
	if spelling, exists := cached.names[directoryFold(part)]; exists && spelling != part {
		return errFileStoreKey
	}
	return nil
}
