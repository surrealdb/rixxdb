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

import "errors"

// These errors can occur when opening or calling methods on a DB.
var (
	// ErrDbClosed occurs when a DB is accessed after it is closed.
	ErrDbClosed = errors.New("DB is not open")

	// ErrDbClosed occurs when a DB path is epcified incorrectly.
	ErrDbPathError = errors.New("DB path error")

	// ErrDbMemoryOnly occurs when persisting an in-memory DB.
	ErrDbMemoryOnly = errors.New("DB is memory-only")

	// ErrDbAlreadySyncing occurs when a sync is already in progress.
	ErrDbAlreadySyncing = errors.New("DB is already syncing")

	// ErrDbAlreadyShrinking occurs when a shrink is already in progress.
	ErrDbAlreadyShrinking = errors.New("DB is already shrinking")

	// ErrDbFileVersionMismatch occurs when the file version is inavlid.
	ErrDbFileVersionMismatch = errors.New("DB file version incorrect")

	// ErrDbFileContentsInvalid occurs when the file is invalid or currupted.
	ErrDbFileContentsInvalid = errors.New("DB file contents invalid")

	// ErrDbInvalidSizePolicy occurs when the provided size policy is invalid.
	ErrDbInvalidSizePolicy = errors.New("DB file size policy invalid")

	// ErrDbInvalidEncryptionKey occurs when the provided encryption key is invalid.
	ErrDbInvalidEncryptionKey = errors.New("DB encryption key invalid")
)

// These errors can occur when beginning or calling methods on a TX.
var (
	// ErrTxClosed occurs when cancelling or committing a closed transaction.
	ErrTxClosed = errors.New("TX is closed")

	// ErrTxNotWritable occurs when writing or committing a read-only transaction.
	ErrTxNotWritable = errors.New("TX is not writable")

	// ErrTxNotEditable occurs when calling manually closing a managed transaction.
	ErrTxNotEditable = errors.New("TX is not editable")

	// ErrTxIgnoringDurability occurs when a transaction sync is buffered while shrinking.
	ErrTxIgnoringDurability = errors.New("TX durability ignored")

	// ErrKvNotExpectedValue occurs when using a nil key in put, select, or delete methods.
	ErrTxKeyCanNotBeNil = errors.New("TX key can not be nil")

	// ErrKvNotExpectedValue occurs when conditionally putting or deleting a key-value item.
	ErrTxNotExpectedValue = errors.New("KV val is not expected value")
)
