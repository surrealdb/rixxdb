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

package data

// Item represents an item in a time-series list.
type Item struct {
	ver  uint64
	val  []byte
	list *List
	prev *Item
	next *Item
}

// Ver returns the version of this item in the containing list.
func (i *Item) Ver() uint64 {
	return i.ver
}

// Val returns the value of this item in the containing list.
func (i *Item) Val() []byte {
	return i.val
}

// Set updates the value of this item in the containing list.
func (i *Item) Set(val []byte) *Item {
	i.val = val
	return i
}

// Del deletes the item from any containing list and returns it.
func (i *Item) Del() *Item {

	if i.list != nil {

		i.list.lock.Lock()
		defer i.list.lock.Unlock()

		if i.prev != nil && i.next != nil {
			i.prev.next = i.next
			i.next.prev = i.prev
			i.prev = nil
			i.next = nil
		} else if i.prev != nil {
			i.list.max = i.prev
			i.prev.next = nil
			i.prev = nil
		} else if i.next != nil {
			i.list.min = i.next
			i.next.prev = nil
			i.next = nil
		} else {
			i.list.min = nil
			i.list.max = nil
		}

		i.list.size--

		i.list = nil

	}

	return i

}

// Prev returns the previous item to this item in the list.
func (i *Item) Prev() *Item {

	if i.prev != nil {

		i.list.lock.RLock()
		defer i.list.lock.RUnlock()

		return i.prev

	}

	return nil

}

// Next returns the next item to this item in the list.
func (i *Item) Next() *Item {

	if i.next != nil {

		i.list.lock.RLock()
		defer i.list.lock.RUnlock()

		return i.next

	}

	return nil

}
