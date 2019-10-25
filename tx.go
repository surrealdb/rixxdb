// Copyright © 2019 Abcum Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rixxdb

import (
	"bufio"
	"bytes"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/abcum/rixxdb/data"
)

// TX represents a transaction for modifying the contents of the database.
// A TX is not safe to use from multiple goroutines concurrently. A TX
// emits the events `cancel` and `commit` when the transaction is
// cancelled and committed respectively.
type TX struct {
	db   *DB
	rw   bool
	fn   bool
	ops  []*op
	tree *data.Copy
	lock sync.RWMutex
}

// Closed returns whether the transaction has been closed.
func (tx *TX) Closed() bool {

	return (tx.db == nil)

}

// Cancel cancels the transaction.
func (tx *TX) Cancel() error {

	if tx.fn {
		return ErrTxNotEditable
	}

	return tx.cancel()

}

// Commit commits the transaction.
func (tx *TX) Commit() error {

	if tx.fn {
		return ErrTxNotEditable
	}

	return tx.commit()

}

func (tx *TX) cancel() error {

	defer func() {
		tx.db, tx.ops, tx.tree = nil, nil, nil
	}()

	// If this transaction no longer has
	// a vtree then it has already been
	// closed, so return an error.

	if tx.db == nil {
		return ErrTxClosed
	}

	// If this transaction is not a
	// writable transaction then we can
	// cancell immediately without error.

	if tx.rw == false {
		return nil
	}

	// If this transaction is writable
	// then unlock the mutex so other
	// write transactions can be created.

	tx.db.lock.Unlock()

	return nil

}

func (tx *TX) commit() error {

	defer func() {
		tx.db, tx.ops, tx.tree = nil, nil, nil
	}()

	// If this transaction no longer has
	// a vtree then it has already been
	// closed, so return an error.

	if tx.db == nil {
		return ErrTxClosed
	}

	// If this transaction is not a
	// writable transaction then we can
	// not commit, so return an error.

	if tx.rw == false {
		return ErrTxNotWritable
	}

	// If this transaction is writable
	// then unlock the mutex so other
	// write transactions can be created.

	defer tx.db.lock.Unlock()

	// Pass the transaction operations
	// to the database so that it can
	// write them to persistent storage.

	if err := tx.db.push(tx.ops); err != nil {
		return err
	}

	// If the transaction successfully
	// synced with the database file
	// thenn swap in the new vtree.

	atomic.StorePointer(&tx.db.tree, unsafe.Pointer(tx.tree.Tree()))

	return nil

}

func (tx *TX) forced() error {

	defer func() {
		tx.db, tx.ops, tx.tree = nil, nil, nil
	}()

	// If this transaction no longer has
	// a vtree then it has already been
	// closed, so return an error.

	if tx.db == nil {
		return ErrTxClosed
	}

	// If this transaction is not a
	// writable transaction then we can
	// not commit, so return an error.

	if tx.rw == false {
		return ErrTxNotWritable
	}

	// If this transaction is writable
	// then unlock the mutex so other
	// write transactions can be created.

	defer tx.db.lock.Unlock()

	// If the transaction successfully
	// synced with the database file
	// then swap in the new vtree.

	atomic.StorePointer(&tx.db.tree, unsafe.Pointer(tx.tree.Tree()))

	return nil

}

// All retrieves a single key:value item.
func (tx *TX) All(key []byte) (kvs []*KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	if l := tx.tree.All(key); l != nil {
		l.Walk(func(i *data.Item) bool {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: key, val: v})
			return false
		})
	}

	return

}

// AllL retrieves the range of rows which are prefixed with `key`.
func (tx *TX) AllL(key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	tx.tree.Root().Subs(key, func(k []byte, l *data.List) (e bool) {
		l.Walk(func(i *data.Item) bool {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: key, val: v})
			return false
		})
		max--
		return
	})

	return

}

// AllP retrieves the range of rows which are prefixed with `key`.
func (tx *TX) AllP(key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	c := tx.tree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		l.Walk(func(i *data.Item) bool {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: key, val: v})
			return false
		})
		max--
	}

	return

}

// AllR retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) AllR(beg, end []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	c := tx.tree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			l.Walk(func(i *data.Item) bool {
				if v, err = tx.dec(i.Val()); err != nil {
					return true
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				return false
			})
			max--
		}

	case d >= 1:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			l.Walk(func(i *data.Item) bool {
				if v, err = tx.dec(i.Val()); err != nil {
					return true
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				return false
			})
			max--
		}

	}

	return

}

// Clr removes a single key:value item.
func (tx *TX) Clr(key []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	kv = &KV{key: key}

	if i := tx.tree.Cut(key); i != nil {
		if v, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
	}

	tx.clr(key)

	return

}

// ClrL removes the range of rows which are prefixed with `key`.
func (tx *TX) ClrL(key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	tx.tree.Root().Subs(key, func(k []byte, l *data.List) (e bool) {
		if i := l.Max(); i != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.tree.Cut(k)
			tx.clr(k)
			max--
		}
		return
	})

	return

}

// ClrP removes the range of rows which are prefixed with `key`.
func (tx *TX) ClrP(key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	c := tx.tree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if i := l.Max(); i != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return nil, err
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.tree.Cut(k)
			tx.clr(k)
			max--
		}
	}

	return

}

// ClrR removes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) ClrR(beg, end []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	c := tx.tree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if i := l.Max(); i != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.tree.Cut(k)
				tx.clr(k)
				max--
			}
		}

	case d >= 1:

		k, l := c.Seek(beg)
		if k == nil {
			k, l = c.Last()
		}

		for ; max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if i := l.Max(); i != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.tree.Cut(k)
				tx.clr(k)
				max--
			}
		}

	}

	return

}

// Get retrieves a single key:value item.
func (tx *TX) Get(ver uint64, key []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	kv = &KV{key: key}

	if i := tx.tree.Get(ver, key); i != nil {
		if v, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
	}

	return

}

// GetL retrieves the range of rows which are prefixed with `key`.
func (tx *TX) GetL(ver uint64, key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	tx.tree.Root().Subs(key, func(k []byte, l *data.List) (e bool) {
		if i := l.Get(ver, data.Upto); i != nil && i.Val() != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			max--
		}
		return
	})

	return

}

// GetP retrieves the range of rows which are prefixed with `key`.
func (tx *TX) GetP(ver uint64, key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	c := tx.tree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if i := l.Get(ver, data.Upto); i != nil && i.Val() != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return nil, err
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			max--
		}
	}

	return

}

// GetR retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) GetR(ver uint64, beg, end []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.RLock()
	defer tx.lock.RUnlock()

	c := tx.tree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if i := l.Get(ver, data.Upto); i != nil && i.Val() != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				max--
			}
		}

	case d >= 1:

		k, l := c.Seek(beg)
		if k == nil {
			k, l = c.Last()
		}

		for ; max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if i := l.Get(ver, data.Upto); i != nil && i.Val() != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				max--
			}
		}

	}

	return

}

// Del deletes a single key:value item.
func (tx *TX) Del(ver uint64, key []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	kv = &KV{key: key}

	if i := tx.tree.Del(ver, key); i != nil {
		if v, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
	}

	tx.del(ver, key)

	return

}

// DelC conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *TX) DelC(ver uint64, key, exp []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	var now []byte

	// Get the item at the key

	if i := tx.tree.Get(ver, key); i != nil {
		if now, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
	}

	// Check if the values match

	if !alter(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	kv = &KV{key: key}

	if i := tx.tree.Del(ver, key); i != nil {
		if v, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
	}

	tx.del(ver, key)

	return

}

// DelL deletes the range of rows which are prefixed with `key`.
func (tx *TX) DelL(ver uint64, key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	tx.tree.Root().Subs(key, func(k []byte, l *data.List) (e bool) {
		if i := l.Put(ver, nil); i != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.del(ver, k)
			max--
		}
		return
	})

	return

}

// DelP deletes the range of rows which are prefixed with `key`.
func (tx *TX) DelP(ver uint64, key []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	c := tx.tree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if i := l.Put(ver, nil); i != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return nil, err
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.del(ver, k)
			max--
		}
	}

	return

}

// DelR deletes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) DelR(ver uint64, beg, end []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	c := tx.tree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if i := l.Put(ver, nil); i != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.del(ver, k)
				max--
			}
		}

	case d >= 1:

		k, l := c.Seek(beg)
		if k == nil {
			k, l = c.Last()
		}

		for ; max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if i := l.Put(ver, nil); i != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.del(ver, k)
				max--
			}
		}

	}

	return

}

// Put sets the value for a key.
func (tx *TX) Put(ver uint64, key, val []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.enc(val); err != nil {
		return nil, err
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	kv = &KV{key: key}

	if i := tx.tree.Put(ver, key, val); i != nil {
		if v, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
	}

	tx.put(ver, key, val)

	return

}

// PutC conditionally sets the value for a key if the existing value is
// equal to the expected value. To conditionally set a value only if there
// is no existing entry pass nil for the expected value.
func (tx *TX) PutC(ver uint64, key, val, exp []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.enc(val); err != nil {
		return nil, err
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	var now []byte

	// Get the item at the key

	if i := tx.tree.Get(ver, key); i != nil {
		if now, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
	}

	// Check if the values match

	if !check(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	kv = &KV{key: key}

	if i := tx.tree.Put(ver, key, val); i != nil {
		if v, err = tx.dec(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
	}

	tx.put(ver, key, val)

	return

}

// PutL updates the range of rows which are prefixed with `key`.
func (tx *TX) PutL(ver uint64, key, val []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.enc(val); err != nil {
		return nil, err
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	tx.tree.Root().Subs(key, func(k []byte, l *data.List) (e bool) {
		if i := l.Put(ver, val); i != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.put(ver, k, val)
			max--
		}
		return
	})

	return

}

// PutP updates the range of rows which are prefixed with `key`.
func (tx *TX) PutP(ver uint64, key, val []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.enc(val); err != nil {
		return nil, err
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	c := tx.tree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if i := l.Put(ver, val); i != nil {
			if v, err = tx.dec(i.Val()); err != nil {
				return nil, err
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.put(ver, k, val)
			max--
		}
	}

	return

}

// PutR updates the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) PutR(ver uint64, beg, end, val []byte, max uint64) (kvs []*KV, err error) {

	var v []byte

	if max == 0 {
		max = math.MaxUint64
	}

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.rw {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.enc(val); err != nil {
		return nil, err
	}

	tx.lock.Lock()
	defer tx.lock.Unlock()

	c := tx.tree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if i := l.Put(ver, val); i != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.put(ver, k, val)
				max--
			}
		}

	case d >= 1:

		k, l := c.Seek(beg)
		if k == nil {
			k, l = c.Last()
		}

		for ; max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if i := l.Put(ver, val); i != nil {
				if v, err = tx.dec(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.put(ver, k, val)
				max--
			}
		}

	}

	return

}

// ----------------------------------------------------------------------

func (tx *TX) dec(src []byte) (dst []byte, err error) {
	if dst, err = decrypt(tx.db.conf.EncryptionKey, src); err != nil {
		return nil, ErrDbInvalidEncryptionKey
	}
	return
}

func (tx *TX) enc(src []byte) (dst []byte, err error) {
	if dst, err = encrypt(tx.db.conf.EncryptionKey, src); err != nil {
		return nil, ErrDbInvalidEncryptionKey
	}
	return
}

func (tx *TX) put(ver uint64, key, val []byte) {

	if tx.db.file.pntr == nil {
		return
	}

	tx.ops = append(tx.ops, &op{
		op: put, ver: ver, key: key, val: val,
	})

}

func (tx *TX) del(ver uint64, key []byte) {

	if tx.db.file.pntr == nil {
		return
	}

	tx.ops = append(tx.ops, &op{
		op: del, ver: ver, key: key,
	})

}

func (tx *TX) clr(key []byte) {

	if tx.db.file.pntr == nil {
		return
	}

	tx.ops = append(tx.ops, &op{
		op: clr, key: key,
	})

}

func (tx *TX) out(ver uint64, key, val []byte) (out []byte) {

	out = append(out, 'P')
	out = append(out, wver(ver)...)
	out = append(out, wlen(key)...)
	out = append(out, key...)
	out = append(out, wlen(val)...)
	out = append(out, val...)
	out = append(out, '\n')

	return

}

func (tx *TX) inj(r io.Reader) error {

	b := bufio.NewReaderSize(r, 100*1024*1024)

	for {

		var bit byte
		var err error
		var ver uint64
		var key []byte
		var val []byte

		if bit, err = rbit(b); err == io.EOF {
			break
		}

		switch bit {

		case '\n':
			continue

		case 'E':

			tx.tree = data.New().Copy()

			continue

		case 'C':

			if key, err = rkey(b); err != nil {
				return err
			}

			tx.tree.Cut(key)

			continue

		case 'D':

			if ver, err = rint(b); err != nil {
				return err
			}
			if key, err = rkey(b); err != nil {
				return err
			}

			tx.tree.Del(ver, key)

			continue

		case 'P':

			if ver, err = rint(b); err != nil {
				return err
			}
			if key, err = rkey(b); err != nil {
				return err
			}
			if val, err = rval(b); err != nil {
				return err
			}

			if len(val) == 0 {
				val = nil
			}

			tx.tree.Put(ver, key, val)

			continue

		}

		return ErrDbFileContentsInvalid

	}

	return tx.forced()

}
