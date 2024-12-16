package db

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/btree"
)

// DelayedMemDB is an in-memory database backend using a B-tree for testing purposes.
// It delays the Write() operation by the specified duration.
//
// For performance reasons, all given and returned keys and values are pointers to the in-memory
// database, so modifying them will cause the stored values to be modified as well. All DB methods
// already specify that keys and values should be considered read-only, but this is especially
// important with DelayedMemDB.
type DelayedMemDB struct {
	delay time.Duration
	mtx   sync.RWMutex
	btree *btree.BTree
}

var _ DB = (*DelayedMemDB)(nil)

// NewDelayedMemDB creates a new in-memory database.
func NewDelayedMemDB(delay time.Duration) *DelayedMemDB {
	database := &DelayedMemDB{
		delay: delay,
		btree: btree.New(bTreeDegree),
	}
	return database
}

// Get implements DB.
func (db *DelayedMemDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	i := db.btree.Get(newKey(key))
	if i != nil {
		return i.(item).value, nil
	}
	return nil, nil
}

// Has implements DB.
func (db *DelayedMemDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	return db.btree.Has(newKey(key)), nil
}

// Set implements DB.
func (db *DelayedMemDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.set(key, value)
	return nil
}

// set sets a value without locking the mutex.
func (db *DelayedMemDB) set(key []byte, value []byte) {
	db.btree.ReplaceOrInsert(newPair(key, value))
}

// SetSync implements DB.
func (db *DelayedMemDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *DelayedMemDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.delete(key)
	return nil
}

// delete deletes a key without locking the mutex.
func (db *DelayedMemDB) delete(key []byte) {
	db.btree.Delete(newKey(key))
}

// DeleteSync implements DB.
func (db *DelayedMemDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Close implements DB.
func (db *DelayedMemDB) Close() error {
	// Close is a noop since for an in-memory database, we don't have a destination to flush
	// contents to nor do we want any data loss on invoking Close().
	// See the discussion in https://github.com/tendermint/tendermint/libs/pull/56
	return nil
}

// Print implements DB.
func (db *DelayedMemDB) Print() error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.btree.Ascend(func(i btree.Item) bool {
		item := i.(item)
		fmt.Printf("[%X]:\t[%X]\n", item.key, item.value)
		return true
	})
	return nil
}

// Stats implements DB.
func (db *DelayedMemDB) Stats() map[string]string {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	stats := make(map[string]string)
	stats["database.type"] = "DelayedMemDB"
	stats["database.size"] = fmt.Sprintf("%d", db.btree.Len())
	return stats
}

// NewBatch implements DB.
func (db *DelayedMemDB) NewBatch() Batch {
	return newDelayedMemDBBatch(db)
}

// NewBatchWithSize implements DB.
// It does the same thing as NewBatch because we can't pre-allocate delayedMemDBBatch
func (db *DelayedMemDB) NewBatchWithSize(_ int) Batch {
	return newDelayedMemDBBatch(db)
}

// Iterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *DelayedMemDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newDelayedMemDBIterator(db, start, end, false), nil
}

// ReverseIterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *DelayedMemDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newDelayedMemDBIterator(db, start, end, true), nil
}

// IteratorNoMtx makes an iterator with no mutex.
func (db *DelayedMemDB) IteratorNoMtx(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newDelayedMemDBIteratorMtxChoice(db, start, end, false, false), nil
}

// ReverseIteratorNoMtx makes an iterator with no mutex.
func (db *DelayedMemDB) ReverseIteratorNoMtx(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newDelayedMemDBIteratorMtxChoice(db, start, end, true, false), nil
}

// delayedMemDBIterator is a memDB iterator.
type delayedMemDBIterator struct {
	ch     <-chan *item
	cancel context.CancelFunc
	item   *item
	start  []byte
	end    []byte
	useMtx bool
}

var _ Iterator = (*delayedMemDBIterator)(nil)

// newDelayedMemDBIterator creates a new delayedMemDBIterator.
func newDelayedMemDBIterator(db *DelayedMemDB, start []byte, end []byte, reverse bool) *delayedMemDBIterator {
	return newDelayedMemDBIteratorMtxChoice(db, start, end, reverse, true)
}

func newDelayedMemDBIteratorMtxChoice(db *DelayedMemDB, start []byte, end []byte, reverse bool, useMtx bool) *delayedMemDBIterator {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *item, chBufferSize)
	iter := &delayedMemDBIterator{
		ch:     ch,
		cancel: cancel,
		start:  start,
		end:    end,
		useMtx: useMtx,
	}

	if useMtx {
		db.mtx.RLock()
	}
	go func() {
		if useMtx {
			defer db.mtx.RUnlock()
		}
		// Because we use [start, end) for reverse ranges, while btree uses (start, end], we need
		// the following variables to handle some reverse iteration conditions ourselves.
		var (
			skipEqual     []byte
			abortLessThan []byte
		)
		visitor := func(i btree.Item) bool {
			item := i.(item)
			if skipEqual != nil && bytes.Equal(item.key, skipEqual) {
				skipEqual = nil
				return true
			}
			if abortLessThan != nil && bytes.Compare(item.key, abortLessThan) == -1 {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case ch <- &item:
				return true
			}
		}
		switch {
		case start == nil && end == nil && !reverse:
			db.btree.Ascend(visitor)
		case start == nil && end == nil && reverse:
			db.btree.Descend(visitor)
		case end == nil && !reverse:
			// must handle this specially, since nil is considered less than anything else
			db.btree.AscendGreaterOrEqual(newKey(start), visitor)
		case !reverse:
			db.btree.AscendRange(newKey(start), newKey(end), visitor)
		case end == nil:
			// abort after start, since we use [start, end) while btree uses (start, end]
			abortLessThan = start
			db.btree.Descend(visitor)
		default:
			// skip end and abort after start, since we use [start, end) while btree uses (start, end]
			skipEqual = end
			abortLessThan = start
			db.btree.DescendLessOrEqual(newKey(end), visitor)
		}
		close(ch)
	}()

	// prime the iterator with the first value, if any
	if item, ok := <-ch; ok {
		iter.item = item
	}

	return iter
}

// Close implements Iterator.
func (i *delayedMemDBIterator) Close() error {
	i.cancel()
	for range i.ch { //nolint:revive
	} // drain channel
	i.item = nil
	return nil
}

// Domain implements Iterator.
func (i *delayedMemDBIterator) Domain() ([]byte, []byte) {
	return i.start, i.end
}

// Valid implements Iterator.
func (i *delayedMemDBIterator) Valid() bool {
	return i.item != nil
}

// Next implements Iterator.
func (i *delayedMemDBIterator) Next() {
	i.assertIsValid()
	item, ok := <-i.ch
	switch {
	case ok:
		i.item = item
	default:
		i.item = nil
	}
}

// Error implements Iterator.
func (i *delayedMemDBIterator) Error() error {
	return nil // famous last words
}

// Key implements Iterator.
func (i *delayedMemDBIterator) Key() []byte {
	i.assertIsValid()
	return i.item.key
}

// Value implements Iterator.
func (i *delayedMemDBIterator) Value() []byte {
	i.assertIsValid()
	return i.item.value
}

func (i *delayedMemDBIterator) assertIsValid() {
	if !i.Valid() {
		panic("iterator is invalid")
	}
}

// delayedMemDBBatch handles in-memory batching.
type delayedMemDBBatch struct {
	db   *DelayedMemDB
	ops  []operation
	size int
}

var _ Batch = (*delayedMemDBBatch)(nil)

// newDelayedMemDBBatch creates a new delayedMemDBBatch
func newDelayedMemDBBatch(db *DelayedMemDB) *delayedMemDBBatch {
	return &delayedMemDBBatch{
		db:   db,
		ops:  []operation{},
		size: 0,
	}
}

// Set implements Batch.
func (b *delayedMemDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.size += len(key) + len(value)
	b.ops = append(b.ops, operation{opTypeSet, key, value})
	return nil
}

// Delete implements Batch.
func (b *delayedMemDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.size += len(key)
	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
	return nil
}

// Write implements Batch.
func (b *delayedMemDBBatch) Write() error {
	if b.ops == nil {
		return errBatchClosed
	}
	b.db.mtx.Lock()
	defer b.db.mtx.Unlock()

	go func(ops []operation) {
		time.Sleep(b.db.delay)

		b.db.mtx.Lock()
		defer b.db.mtx.Unlock()

		for _, op := range ops {
			switch op.opType {
			case opTypeSet:
				b.db.set(op.key, op.value)
			case opTypeDelete:
				b.db.delete(op.key)
			default:
				panic(fmt.Sprintf("unknown operation type %v (%v)", op.opType, op))
			}
		}
	}(b.ops)

	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// WriteSync implements Batch.
func (b *delayedMemDBBatch) WriteSync() error {
	if b.ops == nil {
		return errBatchClosed
	}
	b.db.mtx.Lock()
	defer b.db.mtx.Unlock()

	for _, op := range b.ops {
		switch op.opType {
		case opTypeSet:
			b.db.set(op.key, op.value)
		case opTypeDelete:
			b.db.delete(op.key)
		default:
			return fmt.Errorf("unknown operation type %v (%v)", op.opType, op)
		}
	}

	return b.Close()
}

// Close implements Batch.
func (b *delayedMemDBBatch) Close() error {
	b.ops = nil
	b.size = 0
	return nil
}

// GetByteSize implements Batch
func (b *delayedMemDBBatch) GetByteSize() (int, error) {
	if b.ops == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}
