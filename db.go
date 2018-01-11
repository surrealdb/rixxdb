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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/abcum/ptree"
	"github.com/abcum/syncr"
	"github.com/abcum/tlist"
)

// DB represents a database which operates in memory, and persists to
// disk. A DB is safe to use from multiple goroutines concurrently. A
// DB can have only one read-write transaction open at a time, but
// allows an unlimited number of concurrent read-only transactions at
// a time, each with its own consistent view of the data as it existed
// when the transaction started.
type DB struct {
	open bool
	path string
	conf *Config
	lock sync.Mutex
	tree unsafe.Pointer
	wait struct {
		sync bool
		shrk bool
	}
	tick struct {
		sync *time.Ticker
		shrk *time.Ticker
	}
	buff struct {
		lock sync.Mutex
		pntr *bytes.Buffer
	}
	send struct {
		lock sync.Mutex
		pntr *bufio.Writer
	}
	file struct {
		lock sync.Mutex
		pntr syncr.Syncable
	}
}

// Open creates and opens a database at the given path. If the file
// does not exist then it will be created automatically. Passing
// in a nil Config will cause Rixx to use the default options.
func Open(path string, conf *Config) (*DB, error) {

	db := &DB{
		open: true,
		path: path,
		conf: conf,
		tree: unsafe.Pointer(ptree.New()),
	}

	// Check that if there is an encryption key specified
	// on DB creation, that the key is of the correct length
	// for AES-128, AES-192, or AES-256 encryption.

	if conf.EncryptionKey != nil {
		if l := len(conf.EncryptionKey); l != 16 && l != 24 && l != 32 {
			return nil, ErrDbInvalidEncryptionKey
		}
	}

	// If the database has been specified to sync to disk
	// then setup the syncr process, and read any data off
	// the stream before enabling writing to storage.

	if path != "memory" {

		var err error

		if db.file.pntr, err = persist(path, conf); err != nil {
			return nil, err
		}

		// Go back to beg of file for reading.
		if _, err := db.file.pntr.Seek(0, 0); err != nil {
			db.file.pntr.Close()
			return nil, err
		}

		if err := db.Load(db.file.pntr); err != nil {
			db.file.pntr.Close()
			return nil, err
		}

		// Go back to end of file for writing.
		if _, err := db.file.pntr.Seek(0, 2); err != nil {
			db.file.pntr.Close()
			return nil, err
		}

		db.buff.pntr = bytes.NewBuffer(nil)
		db.send.pntr = bufio.NewWriter(db.file.pntr)

	}

	go db.sync()
	go db.shrk()

	return db, nil

}

func (db *DB) sync() {

	if db.file.pntr == nil {
		return
	}

	if db.conf.SyncPolicy < 0 {
		return
	}

	if db.conf.SyncPolicy > 0 {

		db.tick.sync = time.NewTicker(db.conf.SyncPolicy)

		defer db.tick.sync.Stop()

		for range db.tick.sync.C {
			if err := db.Sync(); err != nil {
				panic(err)
				break
			}
		}

	}

}

func (db *DB) shrk() {

	if db.file.pntr == nil {
		return
	}

	if db.conf.ShrinkPolicy < 0 {
		return
	}

	if db.conf.ShrinkPolicy > 0 {

		db.tick.shrk = time.NewTicker(db.conf.ShrinkPolicy)

		defer db.tick.shrk.Stop()

		for range db.tick.shrk.C {
			if err := db.Shrink(); err != nil {
				panic(err)
				break
			}
		}

	}

}

func (db *DB) root() *ptree.Tree {
	return (*ptree.Tree)(atomic.LoadPointer(&db.tree))
}

func (db *DB) push(b []byte) error {

	// If there is no file associated
	// with this database then ignore
	// this method call.

	if db.file.pntr == nil {
		return nil
	}

	// If the database SyncPolicy has
	// been disabled, then ignore this
	// call to sync the data buffer.

	if db.conf.SyncPolicy < 0 {
		return nil
	}

	// If the SyncPolicy is specified
	// asynchronous, write the data
	// to the buffer to sync later.

	if db.conf.SyncPolicy >= 0 {

		db.buff.lock.Lock()
		defer db.buff.lock.Unlock()

		if _, err := db.buff.pntr.Write(b); err != nil {
			return err
		}

	}

	// If a job is currently being
	// processed, and we can ignore
	// durability, then don't flush.

	if db.conf.IgnoreSyncPolicyWhenShrinking {
		if db.wait.sync || db.wait.shrk {
			return nil
		}
	}

	// If the SyncPolicy is specified
	// to sync on every commit, then
	// ensure the data is synced now.

	if db.conf.SyncPolicy == 0 {

		db.file.lock.Lock()
		defer db.file.lock.Unlock()

		if _, err := db.buff.pntr.WriteTo(db.send.pntr); err != nil {
			return err
		}

		if err := db.send.pntr.Flush(); err != nil {
			return err
		}

		if err := db.file.pntr.Sync(); err != nil {
			return err
		}

	}

	return nil

}

// Load loads database operations from a reader. This can be used to
// playback a database snapshot into an already running database.
func (db *DB) Load(r io.Reader) error {

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Cancel()

	return tx.inj(r)

}

// Save saves all database operations to a writer. This can be used to
// save a database snapshot to a secondary file or stream.
func (db *DB) Save(w io.Writer) error {

	tx, err := db.Begin(false)
	if err != nil {
		return err
	}

	defer tx.Cancel()

	cur := tx.ptree.Cursor()

	for k, l := cur.First(); k != nil; k, l = cur.Next() {
		l.(*tlist.List).Walk(func(i *tlist.Item) (e bool) {
			w.Write(tx.out(i.Ver(), k, i.Val()))
			return
		})
	}

	return nil

}

// Sync ensures that all database operations are flushed to the
// underlying storage. If the database is currently performing
// a shrink from a previous call to this method, then the call
// will be ignored. This does nothing on in-memory databases.
func (db *DB) Sync() error {

	// If there is no file associated
	// with this database then ignore
	// this method call.

	if db.file.pntr == nil {
		return ErrDbMemoryOnly
	}

	// If the database is currently
	// already syncing, then ignore
	// the sync this time around.

	if db.wait.sync {
		return ErrDbAlreadySyncing
	}

	// Mark that the database is now
	// syncing so that other calls
	// to sync will be ignored.

	db.wait.sync = true

	// Ensure that when this method
	// is finished we mark that the
	// database is not syncing.

	defer func() {
		db.wait.sync = false
	}()

	// Obtain a lock on the buffer to
	// prevent changes while we flush
	// the buffer to the sender.

	db.buff.lock.Lock()
	defer db.buff.lock.Unlock()

	// Obtain a lock on the sender to
	// prevent changes while we flush
	// the sender to the file.

	db.send.lock.Lock()
	defer db.send.lock.Unlock()

	// Obtain a lock on the file to
	// prevent other threads from
	// syncing to the file.

	db.file.lock.Lock()
	defer db.file.lock.Unlock()

	// Flush the buffer to the file
	// and ensure that the file is
	// synced to storage in the OS.

	if _, err := db.buff.pntr.WriteTo(db.send.pntr); err != nil {
		return err
	}

	if err := db.send.pntr.Flush(); err != nil {
		return err
	}

	if err := db.file.pntr.Sync(); err != nil {
		return err
	}

	return nil

}

// Shrink ensures that all unnecessary database operations that
// have been flushed to disk are removed, reducing the output
// of the append-only log files. If the database is currently
// performing a shrink from a previous call to this method,
// then the call will be ignored. This only works for certain
// storage types, and does nothing on in-memory databases.
func (db *DB) Shrink() error {

	// If there is no file associated
	// with this database then ignore
	// this method call.

	if db.file.pntr == nil {
		return ErrDbMemoryOnly
	}

	// If the database is currently
	// already shrinking, then ignore
	// the shrink this time around.

	if db.wait.shrk {
		return ErrDbAlreadyShrinking
	}

	// Mark that the database is now
	// shrinking so that other calls
	// to sync will be ignored.

	db.wait.shrk = true

	// Ensure that when this method
	// is finished we mark that the
	// database is not shrinking.

	defer func() {
		db.wait.shrk = false
	}()

	// Obtain a lock on the sender to
	// prevent changes while we link
	// the send buffer to the file.

	db.send.lock.Lock()
	defer db.send.lock.Unlock()

	// Obtain a lock on the file to
	// prevent other threads from
	// syncing to the file.

	db.file.lock.Lock()
	defer db.file.lock.Unlock()

	// TODO preform a file shrink here

	return nil

}

// Close waits for all transactions to finish and releeases resources.
func (db *DB) Close() error {

	var err error

	if !db.open {
		return ErrDbClosed
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	db.buff.lock.Lock()
	defer db.buff.lock.Unlock()

	db.send.lock.Lock()
	defer db.send.lock.Unlock()

	db.file.lock.Lock()
	defer db.file.lock.Unlock()

	if db.tick.sync != nil {
		db.tick.sync.Stop()
		db.tick.sync = nil
	}

	if db.tick.shrk != nil {
		db.tick.shrk.Stop()
		db.tick.shrk = nil
	}

	defer func() { db.tree, db.path, db.open = nil, "", false }()

	if db.buff.pntr != nil {
		defer func() { db.buff.pntr = nil }()
		if _, err = db.buff.pntr.WriteTo(db.send.pntr); err != nil {
			return err
		}
	}

	if db.send.pntr != nil {
		defer func() { db.send.pntr = nil }()
		if err = db.send.pntr.Flush(); err != nil {
			return err
		}
	}

	if db.file.pntr != nil {
		defer func() { db.file.pntr = nil }()
		if err = db.file.pntr.Close(); err != nil {
			return err
		}
	}

	return nil

}

// Begin starts a new transaction. Multiple read-only transactions can
// be used concurrently but only one write transaction can be used at
// a time. Starting multiple write transactions will cause the calls
// to be serialized until the current write transaction finishes.
func (db *DB) Begin(writeable bool) (*TX, error) {

	if !db.open {
		return nil, ErrDbClosed
	}

	if writeable {
		db.lock.Lock()
	}

	tx := &TX{
		db:    db,
		write: writeable,
		ptree: db.root().Copy(),
	}

	return tx, nil

}

// View executes a function within the context of a managed read-only
// transaction. Any error that is returned from the function is
// returned from the View() method. Attempting to manually rollback
// within the function will cause a panic.
func (db *DB) View(fn func(*TX) error) error {

	tx, err := db.Begin(false)
	if err != nil {
		return err
	}

	// If the executed function panics
	// then ensure that we rollback and
	// clear up this transaction.

	defer func() {
		if tx.db != nil {
			tx.cancel()
		}
	}()

	// Mark the transaction as managed
	// so that any outside code can not
	// manually call Cancel or Commit.

	tx.owned = true

	// Run the defined transaction in the
	// scope of the transactions, and
	// catch any errors received.

	err = fn(tx)

	// Mark the transaction as unmanaged
	// so that we can call the Cancel
	// or Commit methods to finish up.

	tx.owned = false

	// If an error is returned from the
	// executed function, then clear the
	// transaction and return the error.

	if err != nil {
		tx.Cancel()
		return err
	}

	return tx.Cancel()

}

// Update executes a function within the context of a read-write
// managed transaction. If no error is returned from the function
// then the transaction is committed. If an error is returned then
// the entire transaction is rolled back. Any error that is returned
// from the function or returned from the commit is returned from
// the Update() method. Attempting to manually commit or rollback
// within the function will cause a panic.
func (db *DB) Update(fn func(*TX) error) error {

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}

	// If the executed function panics
	// then ensure that we rollback and
	// clear up this transaction.

	defer func() {
		if tx.db != nil {
			tx.cancel()
		}
	}()

	// Mark the transaction as managed
	// so that any outside code can not
	// manually call Cancel or Commit.

	tx.owned = true

	// Run the defined transaction in the
	// scope of the transactions, and
	// catch any errors received.

	err = fn(tx)

	// Mark the transaction as unmanaged
	// so that we can call the Cancel
	// or Commit methods to finish up.

	tx.owned = false

	// If an error is returned from the
	// executed function, then clear the
	// transaction and return the error.

	if err != nil {
		tx.Cancel()
		return err
	}

	return tx.Commit()

}
