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

import (
	"github.com/Workiva/go-datastructures/trie/yfast"
)

var universe = uint64(0)

// Trie represents a y-fast trie.
type Trie struct {
	min  *Item
	max  *Item
	trie *yfast.YFastTrie
}

func newTrie() *Trie {
	return &Trie{
		trie: yfast.New(universe),
	}
}

// Clr clears all of the items from the trie.
func (l *Trie) Clr() {
	l.min = nil
	l.max = nil
	l.trie = yfast.New(universe)
}

// Len returns the total number of items in the trie.
func (l *Trie) Len() int {
	return int(l.trie.Len())
}

// Min returns the first item in the trie.
func (l *Trie) Min() *Item {
	return l.min
}

// Max returns the last item in the trie.
func (l *Trie) Max() *Item {
	return l.max
}

// Walk iterates over the trie starting at the first version, and continuing
// until the walk function returns true.
func (l *Trie) Walk(fn func(*Item) bool) {
	c := l.trie.Iter(0)
	for c.Next() {
		if v := c.Value(); v != nil {
			i := v.(*Item)
			if fn(i) {
				break
			}
		}
	}
}

// Rng iterates over the trie starting at the first version, and continuing
// until the walk function returns true.
func (l *Trie) Rng(beg, end uint64, fn func(*Item) bool) {
	c := l.trie.Iter(beg)
	for c.Next() {
		if v := c.Value(); v != nil {
			i := v.(*Item)
			if i.ver < end && fn(i) {
				break
			}
		}
	}
}

// Put deletes a specific item from the trie, returning the previous item
// if it existed. If it did not exist, a nil value is returned.
func (l *Trie) Put(ver uint64, val []byte) *Item {
	i := &Item{ver: ver, val: val}
	if l.min == nil || ver <= l.min.ver {
		l.min = i
	}
	if l.max == nil || ver >= l.max.ver {
		l.max = i
	}
	if v := l.trie.Insert(i); v[0] != nil {
		return v[0].(*Item)
	}
	return nil
}

// Del deletes a specific item from the trie, returning the previous item
// if it existed. If it did not exist, a nil value is returned.
func (l *Trie) Del(ver uint64) *Item {
	if l.min != nil && ver == l.min.ver {
		l.min = l.Successor(ver + 1)
	}
	if l.max != nil && ver == l.max.ver {
		l.max = l.Predecessor(ver - 1)
	}
	if v := l.trie.Delete(ver); v[0] != nil {
		return v[0].(*Item)
	}
	return nil
}

// Get gets a specific item from the trie. If the exact item does not
// exist in the trie, then a nil value is returned.
func (l *Trie) Get(ver uint64) *Item {
	if v := l.trie.Get(ver); v != nil {
		return v.(*Item)
	}
	return nil
}

// Get gets a specific item from the trie. If the exact item does not
// exist in the trie, then a nil value is returned.
func (l *Trie) Predecessor(ver uint64) *Item {
	if v := l.trie.Predecessor(ver); v != nil {
		return v.(*Item)
	}
	return nil
}

// Get gets a specific item from the trie. If the exact item does not
// exist in the trie, then a nil value is returned.
func (l *Trie) Successor(ver uint64) *Item {
	if v := l.trie.Successor(ver); v != nil {
		return v.(*Item)
	}
	return nil
}
