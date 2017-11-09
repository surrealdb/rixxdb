// Copyright © 2016 Abcum Ltd
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

	"github.com/abcum/ptree"
	"github.com/abcum/tlist"
)

// TX represents a transaction for modifying the contents of the database.
// A TX is not safe to use from multiple goroutines concurrently. A TX
// emits the events `cancel` and `commit` when the transaction is
// cancelled and committed respectively.
type TX struct {
	db    *DB
	write bool
	owned bool
	alter []byte
	ptree *ptree.Copy
	plock sync.RWMutex
}

// Closed returns whether the transaction has been closed.
func (tx *TX) Closed() bool {

	return (tx.db == nil)

}

// Cancel cancels the transaction.
func (tx *TX) Cancel() error {

	if tx.owned {
		return ErrTxNotEditable
	}

	return tx.cancel()

}

// Commit commits the transaction.
func (tx *TX) Commit() error {

	if tx.owned {
		return ErrTxNotEditable
	}

	return tx.commit()

}

func (tx *TX) cancel() error {

	defer func() {
		tx.db, tx.alter, tx.ptree = nil, nil, nil
	}()

	// If this transaction no longer has
	// a vtree then it has already been
	// closed, so return an error.

	if tx.db == nil {
		return ErrTxClosed
	}

	// If this transaction is writable
	// then unlock the mutex so other
	// write transactions can be created.

	if tx.write {
		defer tx.db.lock.Unlock()
	}

	return nil

}

func (tx *TX) commit() error {

	defer func() {
		tx.db, tx.alter, tx.ptree = nil, nil, nil
	}()

	// If this transaction no longer has
	// a vtree then it has already been
	// closed, so return an error.

	if tx.db == nil {
		return ErrTxClosed
	}

	// If this transaction is writable
	// then unlock the mutex so other
	// write transactions can be created.

	if tx.write {
		defer tx.db.lock.Unlock()
	}

	// If this transaction is not a
	// writable transaction then we can
	// not commit, so return an error.

	if !tx.write {
		return ErrTxNotWritable
	}

	// Write the transaction alteration
	// buffer to the file or rollback the
	// transaction if there is an error.

	if err := tx.db.push(tx.alter); err != nil {
		return err
	}

	// If the transaction successfully
	// synced with the database file
	// thenn swap in the new vtree.

	atomic.StorePointer(&tx.db.tree, unsafe.Pointer(tx.ptree.Tree()))

	return nil

}

func (tx *TX) forced() error {

	defer func() {
		tx.db, tx.alter, tx.ptree = nil, nil, nil
	}()

	// If this transaction no longer has
	// a vtree then it has already been
	// closed, so return an error.

	if tx.db == nil {
		return ErrTxClosed
	}

	// If this transaction is writable
	// then unlock the mutex so other
	// write transactions can be created.

	if tx.write {
		defer tx.db.lock.Unlock()
	}

	// If this transaction is not a
	// writable transaction then we can
	// not commit, so return an error.

	if !tx.write {
		return ErrTxNotWritable
	}

	// If the transaction successfully
	// synced with the database file
	// thenn swap in the new vtree.

	atomic.StorePointer(&tx.db.tree, unsafe.Pointer(tx.ptree.Tree()))

	return nil

}

// Clr removes a single key:value item.
func (tx *TX) Clr(key []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	kv = &KV{key: key}

	if l := tx.ptree.Del(key); l != nil {
		if i := l.(*tlist.List).Max(); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
		tx.clr(key)
	}

	return

}

// ClrC conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *TX) ClrC(key, exp []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	var now []byte

	// Get the item at the key

	if l := tx.ptree.Get(key); l != nil {
		if i := l.(*tlist.List).Max(); i != nil {
			if now, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
		}
	}

	// Check if the values match

	if !check(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	kv = &KV{key: key}

	// If there is a tlist then insert

	if l := tx.ptree.Del(key); l != nil {
		if i := l.(*tlist.List).Max(); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
		tx.clr(key)
	}

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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	tx.ptree.Root().Subs(key, func(k []byte, l interface{}) (e bool) {
		if i := l.(*tlist.List).Max(); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return true
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.ptree.Del(k)
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	c := tx.ptree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if i := l.(*tlist.List).Max(); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
			tx.ptree.Del(k)
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	c := tx.ptree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if i := l.(*tlist.List).Max(); i != nil {
				if v, err = tx.get(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.ptree.Del(k)
				tx.clr(k)
				max--
			}
		}

	case d >= 1:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if i := l.(*tlist.List).Max(); i != nil {
				if v, err = tx.get(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.ptree.Del(k)
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

	tx.plock.RLock()
	defer tx.plock.RUnlock()

	kv = &KV{key: key}

	if l := tx.ptree.Get(key); l != nil {
		if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
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

	tx.plock.RLock()
	defer tx.plock.RUnlock()

	tx.ptree.Root().Subs(key, func(k []byte, l interface{}) (e bool) {
		if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil && i.Val() != nil {
			if v, err = tx.get(i.Val()); err != nil {
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

	tx.plock.RLock()
	defer tx.plock.RUnlock()

	c := tx.ptree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil && i.Val() != nil {
			if v, err = tx.get(i.Val()); err != nil {
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

	tx.plock.RLock()
	defer tx.plock.RUnlock()

	c := tx.ptree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil && i.Val() != nil {
				if v, err = tx.get(i.Val()); err != nil {
					return nil, err
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				max--
			}
		}

	case d >= 1:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil && i.Val() != nil {
				if v, err = tx.get(i.Val()); err != nil {
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	kv = &KV{key: key}

	if l := tx.ptree.Get(key); l != nil {
		if i := l.(*tlist.List).Del(ver, tlist.Upto); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
		tx.del(ver, key)
	}

	return

}

// DelC conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *TX) DelC(ver uint64, key, exp []byte) (kv *KV, err error) {

	var v []byte

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	var now []byte

	// Get the item at the key

	l := tx.ptree.Get(key)

	if l != nil {
		if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil {
			if now, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
		}
	}

	// Check if the values match

	if !alter(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	kv = &KV{key: key}

	// If there is a tlist then delete

	if l := tx.ptree.Get(key); l != nil {
		if i := l.(*tlist.List).Del(ver, tlist.Upto); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
		tx.del(ver, key)
	}

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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	tx.ptree.Root().Subs(key, func(k []byte, l interface{}) (e bool) {
		if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
			if i := l.(*tlist.List).Del(ver, tlist.Upto); i != nil {
				if v, err = tx.get(i.Val()); err != nil {
					return true
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.del(ver, k)
				max--
			}
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	c := tx.ptree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
			if i := l.(*tlist.List).Del(ver, tlist.Upto); i != nil {
				if v, err = tx.get(i.Val()); err != nil {
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	c := tx.ptree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
				if i := l.(*tlist.List).Del(ver, tlist.Upto); i != nil {
					if v, err = tx.get(i.Val()); err != nil {
						return nil, err
					}
					kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
					tx.del(ver, k)
					max--
				}
			}
		}

	case d >= 1:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
				if i := l.(*tlist.List).Del(ver, tlist.Upto); i != nil {
					if v, err = tx.get(i.Val()); err != nil {
						return nil, err
					}
					kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
					tx.del(ver, k)
					max--
				}
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	kv = &KV{key: key}

	// Get the item at the key

	l := tx.ptree.Get(key)

	// If there is a tlist then insert

	if l != nil {
		if i := l.(*tlist.List).Put(ver, val); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
		tx.put(ver, key, val)
	}

	// If there is no tlist then create

	if l == nil {
		l := tlist.New()
		i := l.Put(ver, val)
		if v, err = tx.get(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
		tx.ptree.Put(key, l)
		tx.put(ver, key, val)
	}

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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	var now []byte

	// Get the item at the key

	l := tx.ptree.Get(key)

	if l != nil {
		if i := l.(*tlist.List).Get(ver, tlist.Upto); i != nil {
			if now, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
		}
	}

	// Check if the values match

	if !check(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	kv = &KV{key: key}

	// If there is a tlist then insert

	if l != nil {
		if i := l.(*tlist.List).Put(ver, val); i != nil {
			if v, err = tx.get(i.Val()); err != nil {
				return nil, err
			}
			kv.ver, kv.val = i.Ver(), v
		}
		tx.put(ver, key, val)
	}

	// If there is no tlist then create

	if l == nil {
		l := tlist.New()
		i := l.Put(ver, val)
		if v, err = tx.get(i.Val()); err != nil {
			return nil, err
		}
		kv.ver, kv.val = i.Ver(), v
		tx.ptree.Put(key, l)
		tx.put(ver, key, val)
	}

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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	tx.ptree.Root().Subs(key, func(k []byte, l interface{}) (e bool) {
		if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
			if i := l.(*tlist.List).Put(ver, val); i != nil {
				if v, err = tx.get(i.Val()); err != nil {
					return true
				}
				kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
				tx.put(ver, k, val)
				max--
			}
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	c := tx.ptree.Cursor()

	for k, l := c.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = c.Next() {
		if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
			if i := l.(*tlist.List).Put(ver, val); i != nil {
				if v, err = tx.get(i.Val()); err != nil {
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

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	tx.plock.Lock()
	defer tx.plock.Unlock()

	c := tx.ptree.Cursor()

	d := bytes.Compare(beg, end)

	switch {

	case d <= 0:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = c.Next() {
			if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
				if i := l.(*tlist.List).Put(ver, val); i != nil {
					if v, err = tx.get(i.Val()); err != nil {
						return nil, err
					}
					kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
					tx.put(ver, k, val)
					max--
				}
			}
		}

	case d >= 1:

		for k, l := c.Seek(beg); max > 0 && k != nil && bytes.Compare(end, k) < 0; k, l = c.Prev() {
			if l.(*tlist.List).Get(ver, tlist.Upto) != nil {
				if i := l.(*tlist.List).Put(ver, val); i != nil {
					if v, err = tx.get(i.Val()); err != nil {
						return nil, err
					}
					kvs = append(kvs, &KV{ver: i.Ver(), key: k, val: v})
					tx.put(ver, k, val)
					max--
				}
			}
		}

	}

	return

}

// ----------------------------------------------------------------------

func (tx *TX) get(src []byte) (dst []byte, err error) {
	if dst, err = decrypt(tx.db.conf.EncryptionKey, src); err != nil {
		return nil, ErrDbInvalidEncryptionKey
	}
	return
}

func (tx *TX) set(src []byte) (dst []byte, err error) {
	if dst, err = encrypt(tx.db.conf.EncryptionKey, src); err != nil {
		return nil, ErrDbInvalidEncryptionKey
	}
	return
}

func (tx *TX) put(ver uint64, key, val []byte) {

	if tx.db.file.pntr == nil {
		return
	}

	tx.alter = append(tx.alter, 'P')
	tx.alter = append(tx.alter, wver(ver)...)
	tx.alter = append(tx.alter, wlen(key)...)
	tx.alter = append(tx.alter, key...)
	tx.alter = append(tx.alter, wlen(val)...)
	tx.alter = append(tx.alter, val...)
	tx.alter = append(tx.alter, '\n')

}

func (tx *TX) del(ver uint64, key []byte) {

	if tx.db.file.pntr == nil {
		return
	}

	tx.alter = append(tx.alter, 'D')
	tx.alter = append(tx.alter, wver(ver)...)
	tx.alter = append(tx.alter, wlen(key)...)
	tx.alter = append(tx.alter, key...)
	tx.alter = append(tx.alter, '\n')

}

func (tx *TX) clr(key []byte) {

	if tx.db.file.pntr == nil {
		return
	}

	tx.alter = append(tx.alter, 'C')
	tx.alter = append(tx.alter, wlen(key)...)
	tx.alter = append(tx.alter, key...)
	tx.alter = append(tx.alter, '\n')

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

	buf := bufio.NewReaderSize(r, 1*1024*1024)

	for {

		var bit byte
		var err error
		var ver uint64
		var key []byte
		var val []byte

		if bit, err = buf.ReadByte(); err == io.EOF {
			break
		}

		switch bit {

		case '\n':
			continue

		case 'C':

			if key, err = rkey(buf); err != nil {
				return err
			}

			tx.ptree.Del(key)

			continue

		case 'D':

			if ver, err = rint(buf); err != nil {
				return err
			}
			if key, err = rkey(buf); err != nil {
				return err
			}

			if l := tx.ptree.Get(key); l != nil {
				l.(*tlist.List).Del(ver, tlist.Upto)
			}

			continue

		case 'P':

			if ver, err = rint(buf); err != nil {
				return err
			}
			if key, err = rkey(buf); err != nil {
				return err
			}
			if val, err = rval(buf); err != nil {
				return err
			}

			if l := tx.ptree.Get(key); l != nil {
				l.(*tlist.List).Put(ver, val)
			} else {
				l := tlist.New()
				l.Put(ver, val)
				tx.ptree.Put(key, l)
			}

			continue

		}

		return ErrDbFileContentsInvalid

	}

	return tx.forced()

}
