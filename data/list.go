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

import "sync"

// List represents a doubly-linked time-series list.
type List struct {
	size int
	min  *Item
	max  *Item
	lock sync.RWMutex
}

// Find determines which method is used to seek items in the list.
type Find int8

const (
	// Exact returns an item at a specific version from the list. If the exact
	// item does not exist in the list, then a nil value is returned.
	Exact Find = iota
	// Prev returns the nearest item in the list, where the version number is
	// less than the given version. In a time-series list, this can be used
	// to get the version that was valid before a specified time.
	Prev
	// Next returns the nearest item in the list, where the version number is
	// greater than the given version. In a time-series list, this can be used
	// to get the version that was changed after a specified time.
	Next
	// Upto returns the nearest item in the list, where the version number is
	// less than or equal to the given version. In a time-series list, this can
	// be used to get the version that was current at the specified time.
	Upto
	// Nearest returns an item nearest a specific version in the list. If there
	// is a previous version to the given version, then it will be returned,
	// otherwise it will return the next available version.
	Nearest
)

// Clr clears all of the items from the list.
func (l *List) Clr() {

	l.lock.Lock()
	defer l.lock.Unlock()

	l.size = 0
	l.min = nil
	l.max = nil

}

// Put inserts a new item into the list, ensuring that the list is sorted
// after insertion. If an item with the same version already exists in the
// list, then the value is updated.
func (l *List) Put(ver uint64, val []byte) *Item {

	l.lock.Lock()
	defer l.lock.Unlock()

	// If there is no min or max for
	// this list, then we can just add
	// this item as the min and max.

	if l.min == nil && l.max == nil {
		i := &Item{ver: ver, val: val, list: l}
		l.min, l.max = i, i
		l.size++
		return i
	}

	//
	//
	//

	i := &Item{ver: ver, val: val, list: l}

	// Otherwise find the nearest item
	// to this version so we can update
	// it or prepend / append to it.

	f := l.find(ver, Nearest)

	if f.ver == ver {
		if f.prev != nil {
			f.prev.next = i
			i.prev = f.prev
		}
		if f.next != nil {
			f.next.prev = i
			i.next = f.next
		}
		// f.val = val
		// return f
	}

	// If the found item version is not
	// the same version as the one we're
	// updating then insert the new item.

	if f.ver < ver {
		if f.next != nil {
			f.next.prev = i
			i.next = f.next
			f.next = i
		}
		i.prev = f
		f.next = i
	}

	if f.ver > ver {
		i.next = f
		f.prev = i
	}

	// If there are no previous items
	// before this item then mark this
	// item as the minimum in the list.

	if i.prev == nil {
		l.min = i
	}

	// If there are no subsequent items
	// after this item then mark this
	// item as the maximum in the list.

	if i.next == nil {
		l.max = i
	}

	// If this was an addition and not
	// and update to the list, then
	// increment the size of the list

	if f.ver != ver {
		l.size++
	}

	return f

}

// Del deletes a specific item from the list, returning the previous item
// if it existed. If it did not exist, a nil value is returned.
func (l *List) Del(ver uint64, meth Find) *Item {

	l.lock.Lock()
	defer l.lock.Unlock()

	i := l.find(ver, meth)

	if i != nil {

		if i.prev != nil && i.next != nil {
			i.prev.next = i.next
			i.next.prev = i.prev
			i.prev = nil
			i.next = nil
		} else if i.prev != nil {
			i.prev.next = nil
			l.max = i.prev
			i.prev = nil
		} else if i.next != nil {
			i.next.prev = nil
			l.min = i.next
			i.next = nil
		} else {
			l.min = nil
			l.max = nil
		}

		i.list = nil

		l.size--

	}

	return i

}

// Exp expunges all items in the list, upto and including the specified
// version, returning the latest version, or a nil value if not found.
func (l *List) Exp(ver uint64, meth Find) *Item {

	l.lock.Lock()
	defer l.lock.Unlock()

	i := l.find(ver, meth)

	if i != nil {

		for now := i; now != nil; now = now.prev {
			l.size--
		}

		if i.next != nil {
			i.next.prev = nil
			l.min = i.next
			i.next = nil
		}

	}

	return i

}

// Get gets a specific item from the list. If the exact item does not
// exist in the list, then a nil value is returned.
func (l *List) Get(ver uint64, meth Find) *Item {

	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.find(ver, meth)

}

// Len returns the total number of items in the list.
func (l *List) Len() int {

	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.size

}

// Min returns the first item in the list. In a time-series list this can be
// used to get the initial version.
func (l *List) Min() *Item {

	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.min

}

// Max returns the last item in the list. In a time-series list this can be
// used to get the latest version.
func (l *List) Max() *Item {

	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.max

}

// Walk iterates over the list starting at the first version, and continuing
// until the walk function returns true.
func (l *List) Walk(fn func(*Item) bool) {

	l.lock.RLock()
	defer l.lock.RUnlock()

	for i := l.min; i != nil && !fn(i); i = i.next {
		continue
	}

}

// Rng iterates over the list starting at the first version, and continuing
// until the walk function returns true.
func (l *List) Rng(beg, end uint64, fn func(*Item) bool) {

	l.lock.RLock()
	defer l.lock.RUnlock()

	for i := l.min; i != nil; i = i.next {
		if i.ver >= beg && i.ver < end && fn(i) {
			return
		}
	}

}

// ---------------------------------------------------------------------------

func (l *List) find(ver uint64, what Find) (i *Item) {

	if l.min == nil && l.max == nil {
		return nil
	}

	switch what {

	case Prev: // Get the item below the specified version

		if i = l.find(ver, Upto); i != nil {
			return i.prev
		}

	case Next: // Get the item above the specified version

		if i = l.find(ver, Upto); i != nil {
			return i.next
		}

	case Upto: // Get the item up to the specified version

		for i = l.max; i != nil && i.ver > ver; i = i.prev {
			// Ignore
		}
		return i

	case Exact: // Get the exact specified version

		for i = l.max; i != nil && i.ver >= ver; i = i.prev {
			if i.ver == ver {
				return i
			}
		}

	case Nearest: // Get the item nearest the specified version

		for i = l.max; i != nil; i = i.prev {
			if i.ver <= ver || i.prev == nil {
				return i
			}
		}

	}

	return nil

}
