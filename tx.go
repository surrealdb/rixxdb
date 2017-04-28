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
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/abcum/emitr"
	"github.com/abcum/vtree"
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
	vtree *vtree.Copy
	emitr *emitr.Emitter
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
		tx.db, tx.emitr = nil, nil
		tx.alter, tx.vtree = nil, nil
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

	// If there are any deferred functions
	// then run them and remove them so
	// that the memory is freed.

	tx.emitr.Emit("cancel")

	return nil

}

func (tx *TX) commit() error {

	defer func() {
		tx.db, tx.emitr = nil, nil
		tx.alter, tx.vtree = nil, nil
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

	atomic.StorePointer(&tx.db.tree, unsafe.Pointer(tx.vtree.Tree()))

	// If there are any deferred functions
	// then run them and remove them so
	// that the memory is freed.

	tx.emitr.Emit("commit")

	return nil

}

func (tx *TX) forced() error {

	defer func() {
		tx.db, tx.emitr = nil, nil
		tx.alter, tx.vtree = nil, nil
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

	atomic.StorePointer(&tx.db.tree, unsafe.Pointer(tx.vtree.Tree()))

	// If there are any deferred functions
	// then run them and remove them so
	// that the memory is freed.

	tx.emitr.Emit("commit")

	return nil

}

// Get retrieves a single key:value item.
func (tx *TX) Get(ver int64, key []byte) (kv *KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if ver == All {
		return nil, ErrTxVersionNotSupported
	}

	kv = &KV{}

	kv.val = tx.vtree.Get(ver, key)

	kv.val, err = tx.get(kv.val)

	return

}

// GetL retrieves the range of rows which are prefixed with `key`.
func (tx *TX) GetL(ver int64, key []byte) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if ver == All {
		return nil, ErrTxVersionNotSupported
	}

	tx.vtree.Root().Subs(key, func(k []byte, l *vtree.List) (exit bool) {
		if i, v := l.Seek(ver); v != nil {

			if v, err = tx.get(v); err != nil {
				return true
			}

			kvs = append(kvs, &KV{ver: i, key: k, val: v})

		}
		return
	})

	return

}

// GetP retrieves the range of rows which are prefixed with `key`.
func (tx *TX) GetP(ver int64, key []byte, max uint64) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if ver == All {
		return nil, ErrTxVersionNotSupported
	}

	if max == 0 {
		max = math.MaxUint64
	}

	iter := tx.vtree.Cursor()

	for k, l := iter.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = iter.Next() {
		if i, v := l.Seek(ver); v != nil {

			if v, err = tx.get(v); err != nil {
				return nil, err
			}

			kvs = append(kvs, &KV{ver: i, key: k, val: v})

			max--

		}
	}

	return

}

// GetR retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) GetR(ver int64, beg, end []byte, max uint64) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if ver == All {
		return nil, ErrTxVersionNotSupported
	}

	if max == 0 {
		max = math.MaxUint64
	}

	iter := tx.vtree.Cursor()

	if bytes.Compare(beg, end) <= 0 {
		for k, l := iter.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = iter.Next() {
			if i, v := l.Seek(ver); v != nil {

				if v, err = tx.get(v); err != nil {
					return nil, err
				}

				kvs = append(kvs, &KV{ver: i, key: k, val: v})

				max--

			}
		}
		return
	}

	if bytes.Compare(beg, end) >= 1 {
		for k, l := iter.Seek(end); max > 0 && k != nil && bytes.Compare(beg, k) < 0; k, l = iter.Prev() {
			if i, v := l.Seek(ver); v != nil {

				if v, err = tx.get(v); err != nil {
					return nil, err
				}

				kvs = append(kvs, &KV{ver: i, key: k, val: v})

				max--

			}
		}
		return
	}

	return

}

// Del deletes a single key:value item.
func (tx *TX) Del(ver int64, key []byte) (kv *KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	kv = &KV{}

	kv.val = tx.vtree.Del(ver, key)

	tx.del(ver, key)

	return

}

// DelC conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *TX) DelC(ver int64, key, exp []byte) (kv *KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	now := tx.vtree.Get(ver, key)

	if now, err = tx.get(now); err != nil {
		return nil, err
	}

	if !alter(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	kv = &KV{}

	kv.val = tx.vtree.Del(ver, key)

	kv.val, err = tx.get(kv.val)

	tx.del(ver, key)

	return

}

// DelL deletes the range of rows which are prefixed with `key`.
func (tx *TX) DelL(ver int64, key []byte) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	tx.vtree.Root().Subs(key, func(k []byte, l *vtree.List) (exit bool) {
		if i, v := l.Seek(ver); v != nil {

			// FIXME add DEL function in here

			if v, err = tx.get(v); err != nil {
				return true
			}

			kvs = append(kvs, &KV{ver: i, key: k, val: v})

			tx.del(ver, k)

		}
		return
	})

	return

}

// DelP deletes the range of rows which are prefixed with `key`.
func (tx *TX) DelP(ver int64, key []byte, max uint64) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if max == 0 {
		max = math.MaxUint64
	}

	iter := tx.vtree.Cursor()

	for k, l := iter.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = iter.Next() {
		if i, v := l.Seek(ver); v != nil {

			if v, err = tx.get(v); err != nil {
				return nil, err
			}

			kvs = append(kvs, &KV{ver: i, key: k, val: v})

			tx.del(ver, k)

			iter.Del()

			max--

		}
	}

	return

}

// DelR deletes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) DelR(ver int64, beg, end []byte, max uint64) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if max == 0 {
		max = math.MaxUint64
	}

	iter := tx.vtree.Cursor()

	if bytes.Compare(beg, end) <= 0 {
		for k, l := iter.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = iter.Next() {
			if i, v := l.Seek(ver); v != nil {

				if v, err = tx.get(v); err != nil {
					return nil, err
				}

				kvs = append(kvs, &KV{ver: i, key: k, val: v})

				tx.del(ver, k)

				iter.Del()

				max--

			}
		}
		return
	}

	if bytes.Compare(beg, end) >= 1 {
		for k, l := iter.Seek(end); max > 0 && k != nil && bytes.Compare(beg, k) < 0; k, l = iter.Prev() {
			if i, v := l.Seek(ver); v != nil {

				if v, err = tx.get(v); err != nil {
					return nil, err
				}

				kvs = append(kvs, &KV{ver: i, key: k, val: v})

				tx.del(ver, k)

				iter.Del()

				max--

			}
		}
		return
	}

	return

}

// Put sets the value for a key.
func (tx *TX) Put(ver int64, key, val []byte) (kv *KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if ver == All || ver == Now {
		return nil, ErrTxVersionNotSupported
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	kv = &KV{}

	kv.val = tx.vtree.Put(ver, key, val)

	tx.put(ver, key, val)

	return

}

// PutC conditionally sets the value for a key if the existing value is
// equal to the expected value. To conditionally set a value only if there
// is no existing entry pass nil for the expected value.
func (tx *TX) PutC(ver int64, key, val, exp []byte) (kv *KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if ver == All || ver == Now {
		return nil, ErrTxVersionNotSupported
	}

	now := tx.vtree.Get(ver, key)

	if now, err = tx.get(now); err != nil {
		return nil, err
	}

	if !check(now, exp) {
		return nil, ErrTxNotExpectedValue
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	kv = &KV{}

	kv.val = tx.vtree.Put(ver, key, val)

	kv.val, err = tx.get(kv.val)

	tx.put(ver, key, val)

	return

}

// PutL updates the range of rows which are prefixed with `key`.
func (tx *TX) PutL(ver int64, key, val []byte) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if ver == All || ver == Now {
		return nil, ErrTxVersionNotSupported
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	tx.vtree.Root().Subs(key, func(k []byte, l *vtree.List) (exit bool) {
		kvs = append(kvs, &KV{ver: ver, key: k, val: val})
		tx.put(ver, k, val)
		l.Put(ver, val)
		return
	})

	return

}

// PutP updates the range of rows which are prefixed with `key`.
func (tx *TX) PutP(ver int64, key, val []byte, max uint64) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if key == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if ver == All || ver == Now {
		return nil, ErrTxVersionNotSupported
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	if max == 0 {
		max = math.MaxUint64
	}

	iter := tx.vtree.Cursor()

	for k, l := iter.Seek(key); max > 0 && bytes.HasPrefix(k, key); k, l = iter.Next() {
		kvs = append(kvs, &KV{ver: ver, key: k, val: val})
		tx.put(ver, k, val)
		l.Put(ver, val)
		max--
	}

	return

}

// PutR updates the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) PutR(ver int64, beg, end, val []byte, max uint64) (kvs []*KV, err error) {

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	if !tx.write {
		return nil, ErrTxNotWritable
	}

	if beg == nil || end == nil {
		return nil, ErrTxKeyCanNotBeNil
	}

	if ver == All || ver == Now {
		return nil, ErrTxVersionNotSupported
	}

	if val, err = tx.set(val); err != nil {
		return nil, err
	}

	if max == 0 {
		max = math.MaxUint64
	}

	iter := tx.vtree.Cursor()

	if bytes.Compare(beg, end) <= 0 {
		for k, l := iter.Seek(beg); max > 0 && k != nil && bytes.Compare(k, end) < 0; k, l = iter.Next() {
			kvs = append(kvs, &KV{ver: ver, key: k, val: val})
			tx.put(ver, k, val)
			l.Put(ver, val)
			max--
		}
		return
	}

	if bytes.Compare(beg, end) >= 1 {
		for k, l := iter.Seek(end); max > 0 && k != nil && bytes.Compare(beg, k) < 0; k, l = iter.Prev() {
			kvs = append(kvs, &KV{ver: ver, key: k, val: val})
			tx.put(ver, k, val)
			l.Put(ver, val)
			max--
		}
		return
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

func (tx *TX) put(ver int64, key, val []byte) {
	tx.alter = append(tx.alter, '$', 'P', 'U', 'T', ' ')
	tx.alter = append(tx.alter, strconv.FormatInt(ver, 10)...)
	tx.alter = append(tx.alter, ' ')
	tx.alter = append(tx.alter, strconv.FormatInt(int64(len(key)), 10)...)
	tx.alter = append(tx.alter, ' ')
	tx.alter = append(tx.alter, key...)
	tx.alter = append(tx.alter, ' ')
	tx.alter = append(tx.alter, strconv.FormatInt(int64(len(val)), 10)...)
	tx.alter = append(tx.alter, ' ')
	tx.alter = append(tx.alter, val...)
	tx.alter = append(tx.alter, '\n')
}

func (tx *TX) del(ver int64, key []byte) {
	tx.alter = append(tx.alter, '$', 'D', 'E', 'L', ' ')
	tx.alter = append(tx.alter, strconv.FormatInt(ver, 10)...)
	tx.alter = append(tx.alter, ' ')
	tx.alter = append(tx.alter, strconv.FormatInt(int64(len(key)), 10)...)
	tx.alter = append(tx.alter, ' ')
	tx.alter = append(tx.alter, key...)
	tx.alter = append(tx.alter, '\n')
}

func (tx *TX) out(ver int64, key, val []byte) (out []byte) {
	out = append(out, '$', 'P', 'U', 'T', ' ')
	out = append(out, strconv.FormatInt(ver, 10)...)
	out = append(out, ' ')
	out = append(out, strconv.FormatInt(int64(len(key)), 10)...)
	out = append(out, ' ')
	out = append(out, key...)
	out = append(out, ' ')
	out = append(out, strconv.FormatInt(int64(len(val)), 10)...)
	out = append(out, ' ')
	out = append(out, val...)
	out = append(out, '\n')
	return
}

func (tx *TX) inj(r io.Reader) error {

	del := []byte("$DEL ")
	put := []byte("$PUT ")
	buf := bufio.NewReaderSize(r, 1*1024*1024)

	for {

		var err error
		var ver int64
		var key []byte
		var val []byte
		var data []byte

		if data, err = buf.ReadBytes(' '); err == io.EOF {
			break
		}

		if bytes.Equal(del, data) {
			if ver, err = readver(buf); err != nil {
				return err
			}
			if key, err = readkey(buf); err != nil {
				return err
			}
			tx.vtree.Del(ver, key)
			continue
		}

		if bytes.Equal(put, data) {
			if ver, err = readver(buf); err != nil {
				return err
			}
			if key, err = readkey(buf); err != nil {
				return err
			}
			if val, err = readval(buf); err != nil {
				return err
			}
			tx.vtree.Put(ver, key, val)
			continue
		}

		return ErrDbFileContentsInvalid

	}

	return tx.forced()

}
