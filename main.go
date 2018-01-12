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
	"time"
)

const (
	// SyncNever is used to prevent syncing of data to disk. When
	// this option is specified, all data is kept in memory, and
	// no the database is run with no durability.
	SyncNever time.Duration = -1
	// ShrinkNever is used to disable shrinking the data file. All
	// exact changes to the database are preserved with this option
	// but the data file can grow larger than the data stored.
	ShrinkNever time.Duration = -1
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SizePolicy defines what the file size should be when writing to
	// streaming storage. For local streaming folder storage, the SizePolicy
	// determines the maximum desired file size before rotation the files,
	// and continuing with a new file. For remote streaming storage such as
	// S3 and GCS, the SizePolicy determines the minimum size of the data
	// to cache, before writing to the remote storage.
	SizePolicy int

	// SyncPolicy defines how often the data is synced to the append-only
	// file on disk. '-1' ensures that the database is kept in-memory
	// with no persistence, '0' ensures that the database is persisted
	// to disk after every commit, and a number greater than 0 ensures
	// that the database is committed to disk after the specified seconds.
	SyncPolicy time.Duration

	// ShrinkPolicy defines how often the database append-only file is
	// compacted, removing redundant log entries. '0' ensures that the
	// database append-only file is never compacted, and a number greater
	// than 0 ensures the database is compacted after the specified seconds.
	ShrinkPolicy time.Duration

	// IgnoreSyncPolicyWhenShrinking enables the ability to continue
	// accepting writes to the database, at the same time as a database
	// shrink is being processed. If this is false, then a write
	// transaction which is set to persist on each commit, will wait for
	// the database to finish the shrink process. If this is true, then the
	// transaction will write to a buffer which will be synced to the disk
	// when the shrinking process has finished, and the transaction will
	// commit successfully without writing to disk immediately.
	IgnoreSyncPolicyWhenShrinking bool

	// EncryptionKey enables the ability to specify an encryption key
	// to be used when storing the input data in the underlying data tree.
	// If the encryption key is specified, it must be either 16, 24, or
	// 32 bytes in length in order to use AES-128, AES-192, or AES-256
	// respectively. If no encryption key is specified then the data
	// will not be encrypted before storage or when writing to disk.
	EncryptionKey []byte
}
