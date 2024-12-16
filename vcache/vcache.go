// Package vcache provides a versioned cache that stores additions and removals for each version,
// allowing querying of a specific key over all versions.
package vcache

import "sync"

// VersionedCache represents a cache that holds key-value data for each blockchain height (version).
// It supports concurrent access and allows querying and pruning of versions.
type VersionedCache struct {
	mu            sync.RWMutex
	changes       map[int64]unsavedChanges
	latestVersion int64
	oldestVersion int64
}

// unsavedChanges holds the additions and removals for a specific version.
type unsavedChanges struct {
	additions *sync.Map
	removals  *sync.Map
}

// NewVersionedCache creates and returns a new instance of VersionedCache.
func NewVersionedCache() *VersionedCache {
	return &VersionedCache{
		changes:       make(map[int64]unsavedChanges),
		latestVersion: -1,
		oldestVersion: -1,
	}
}

// SaveVersion stores the additions and removals for a specific version in the cache.
// If the provided version is greater than the current latest version, it updates the latest version.
func (vc *VersionedCache) SaveVersion(version int64, additions *sync.Map, removals *sync.Map) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vc.changes[version] = unsavedChanges{
		additions: additions,
		removals:  removals,
	}
	if version > vc.latestVersion {
		vc.latestVersion = version
	}
	if vc.oldestVersion == -1 || version < vc.oldestVersion {
		vc.oldestVersion = version
	}
}

// Get retrieves a value from the cache for a specific key.
// It searches from the latest version down to version 0, returning the value if found in additions,
// or nil if found in removals. If the key is not found, it returns false.
func (vc *VersionedCache) Get(key interface{}) (interface{}, bool) {
	vc.mu.RLock()
	latestVersion := vc.latestVersion
	vc.mu.RUnlock()

	for version := latestVersion; version >= 0; version-- {
		vc.mu.RLock()
		changes, exists := vc.changes[version]
		vc.mu.RUnlock()

		if !exists {
			break
		}

		if value, exists := changes.additions.Load(key); exists {
			return value, true
		} else if _, exists := changes.removals.Load(key); exists {
			return nil, true
		}
	}

	return nil, false
}

// RemoveVersionsUpTo removes all versions in the cache that are up to the given version.
// It ensures that versions greater than the specified version are retained.
func (vc *VersionedCache) RemoveVersionsUpTo(version int64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	if vc.oldestVersion > version {
		return
	}

	for v := vc.oldestVersion; v <= version; v++ {
		delete(vc.changes, v)
	}

	vc.oldestVersion = version + 1
}

// RemoveVersionsAbove removes all versions in the cache that are greater than the given version.
func (vc *VersionedCache) RemoveVersionsAbove(version int64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	if vc.latestVersion <= version {
		return
	}

	for v := vc.latestVersion; v > version; v-- {
		delete(vc.changes, v)
	}

	vc.latestVersion = version
}
