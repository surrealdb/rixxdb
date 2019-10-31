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

package data

// Item represents an item in a time-series trie.
type Item struct {
	ver uint64
	val []byte
}

// Key returns the key for this item in the containing trie.
func (i *Item) Key() uint64 {
	return i.ver
}

// Ver returns the version of this item in the containing trie.
func (i *Item) Ver() uint64 {
	return i.ver
}

// Val returns the value of this item in the containing trie.
func (i *Item) Val() []byte {
	return i.val
}
