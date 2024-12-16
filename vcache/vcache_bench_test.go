package vcache

import (
	"crypto/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkVersionedCache(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]struct {
		depth   int
		keySize int
	}{
		"shallow-10 - depth: 10, key size - 10b": {
			depth:   10,
			keySize: 10,
		},
		"shallow-20 - depth: 10, key size - 20b": {
			depth:   10,
			keySize: 20,
		},
		"normal-10 - depth: 100, key size - 10b": {
			depth:   100,
			keySize: 10,
		},
		"normal-20 - depth: 100, key size - 20b": {
			depth:   100,
			keySize: 20,
		},
		"deep-10 - depth: 1000, key size - 10b": {
			depth:   1000,
			keySize: 10,
		},
		"deep-20 - depth: 1000, key size - 20b": {
			depth:   1000,
			keySize: 20,
		},
	}

	for name, tc := range testcases {
		key := string(randBytes(tc.keySize))
		cache := buildCache(tc.depth, key)

		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			key := key + strconv.Itoa(0)
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				// try to fetch last key
				_, found := cache.Get(key)
				require.True(b, found)
			}
		})
	}
}

func buildCache(depth int, key string) *VersionedCache {
	cache := NewVersionedCache()
	for i := 0; i < depth; i++ {
		key := key + strconv.Itoa(i)
		cache.SaveVersion(int64(i), createSyncMap(map[string]interface{}{key: true}), createSyncMap(map[string]interface{}{}))
	}
	return cache
}

func randBytes(length int) []byte {
	key := make([]byte, length)
	// math.rand.Read always returns err=nil
	// we do not need cryptographic randomness for this test:

	rand.Read(key) //nolint:errcheck
	return key
}
