package vcache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper function to create a sync.Map with some initial data
func createSyncMap(data map[string]interface{}) *sync.Map {
	sm := &sync.Map{}
	for k, v := range data {
		sm.Store(k, v)
	}
	return sm
}

func TestVersionedCache(t *testing.T) {
	// Create a new VersionedCache instance
	cache := NewVersionedCache()

	// Define some test data for different versions
	version1Additions := map[string]interface{}{
		"key1": "value1v1",
		"key2": "value2v1",
		"key3": "value3v1",
	}
	version1Removals := map[string]interface{}{
		"key4": true,
	}
	version2Additions := map[string]interface{}{
		"key1": "value1v2",
		"key2": "value2v2",
		"key4": "value4v2",
	}
	version2Removals := map[string]interface{}{
		"key3": true,
	}
	version3Additions := map[string]interface{}{
		"key1": "value1v3",
	}
	version3Removals := map[string]interface{}{
		"key2": true,
	}

	// Save versions with data
	cache.SaveVersion(1, createSyncMap(version1Additions), createSyncMap(version1Removals))
	cache.SaveVersion(2, createSyncMap(version2Additions), createSyncMap(version2Removals))
	cache.SaveVersion(3, createSyncMap(version3Additions), createSyncMap(version3Removals))

	// Test retrieving data (latest version should always be preferred)
	tests := []struct {
		key   interface{}
		want  interface{}
		found bool
	}{
		{"key1", "value1v3", true}, // Latest version
		{"key2", nil, true},        // Latest version
		{"key3", nil, true},        // Version 2
		{"key4", "value4v2", true}, // Version 2
		{"key5", nil, false},       // Non-existent key
	}

	for _, tt := range tests {
		t.Run("Get", func(t *testing.T) {
			got, found := cache.Get(tt.key)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.found, found)
		})
	}

	// Prune versions up to 2 (i.e., remove version 1 and version 2)
	cache.RemoveVersionsUpTo(2)

	// do nothing
	cache.RemoveVersionsUpTo(2)
	cache.RemoveVersionsAbove(3)

	got, found := cache.Get("key3")
	require.Equal(t, nil, got)
	require.Equal(t, false, found)

	got, found = cache.Get("key2")
	require.Equal(t, nil, got)
	require.Equal(t, true, found)

	got, found = cache.Get("key1")
	require.Equal(t, "value1v3", got)
	require.Equal(t, true, found)

	// Clear versions to 2 (i.e. all versions removed)
	cache.RemoveVersionsAbove(2)

	got, found = cache.Get("key1")
	require.Equal(t, nil, got)
	require.Equal(t, false, found)

	got, found = cache.Get("key2")
	require.Equal(t, nil, got)
	require.Equal(t, false, found)
}
