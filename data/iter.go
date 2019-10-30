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
	"bytes"
)

// Cursor represents an iterator that can traverse over all key-value
// pairs in a tree in sorted order. Cursors can be obtained from a
// transaction and are valid as long as the transaction is open.
// Changing data while traversing with a cursor may cause it to be
// invalidated and return unexpected keys and/or values. You must
// reposition your cursor after mutating data.
type Cursor struct {
	tree *Copy
	seek []byte
	nums []int
	path []*Node
}

// Here moves the cursor to the first item in the tree and returns
// its key and value. If the tree is empty then a nil key and value
// are returned.
func (c *Cursor) Here() *List {

	_, val := c.Seek(c.seek)

	return val

}

// Del removes the current item under the cursor from the tree. If
// the cursor has not yet been positioned using First, Last, or Seek,
// then no item is deleted and a nil key and value are returned.
func (c *Cursor) Del() ([]byte, *List) {

	_, val := c.Seek(c.seek)

	c.tree.Del(0, c.seek)

	return c.seek, val

}

// First moves the cursor to the first item in the tree and returns
// its key and value. If the tree is empty then a nil key and value
// are returned.
func (c *Cursor) First() ([]byte, *List) {

	c.nums = c.nums[:0]
	c.path = c.path[:0]

	return c.first(c.tree.root)

}

// Last moves the cursor to the last item in the tree and returns its
// key and value. If the tree is empty then a nil key and value are
// returned.
func (c *Cursor) Last() ([]byte, *List) {

	c.nums = c.nums[:0]
	c.path = c.path[:0]

	return c.last(c.tree.root)

}

// Prev moves the cursor to the previous item in the tree and returns
// its key and value. If the tree is empty then a nil key and value are
// returned, and if the cursor is at the start of the tree then a nil key
// and value are returned. If the cursor has not yet been positioned
// using First, Last, or Seek, then a nil key and value are returned.
func (c *Cursor) Prev() ([]byte, *List) {

OUTER:
	for {

		if len(c.path) == 0 {
			break
		}

		// ------------------------------
		// Decrease counter
		// ------------------------------

		for {

			x := len(c.nums) - 1

			if c.nums[x] == 0 {

				c.nums = c.nums[:x]
				c.path = c.path[:x]

				if len(c.nums) == 0 {
					break OUTER
				}

				n := c.node()

				if n.isLeaf() {
					c.seek = n.leaf.key
					return n.leaf.key, n.leaf.val
				}

				continue

			}

			break

		}

		// ------------------------------
		// Decrease edges
		// ------------------------------

		for {

			x := len(c.nums) - 1

			if c.nums[x]-1 >= 0 {

				c.nums[x]--

				n := c.node()

				for {

					if num := len(n.edges); num > 0 {
						c.nums = append(c.nums, num-1)
						c.path = append(c.path, n)
						n = n.edges[num-1]
						continue
					}

					if n.isLeaf() {
						c.seek = n.leaf.key
						return n.leaf.key, n.leaf.val
					}

					continue OUTER

				}

			}

		}

	}

	return nil, nil

}

// Next moves the cursor to the next item in the tree and returns its
// key and value. If the tree is empty then a nil key and value are
// returned, and if the cursor is at the end of the tree then a nil key
// and value are returned. If the cursor has not yet been positioned
// using First, Last, or Seek, then a nil key and value are returned.
func (c *Cursor) Next() ([]byte, *List) {

OUTER:
	for {

		if len(c.nums) == 0 {
			break
		}

		n := c.node()

		// ------------------------------
		// Increase edges
		// ------------------------------

		for {

			if len(n.edges) > 0 {

				c.nums = append(c.nums, 0)
				c.path = append(c.path, n)
				n = n.edges[0]

				if n.isLeaf() {
					c.seek = n.leaf.key
					return n.leaf.key, n.leaf.val
				}

				continue

			}

			break

		}

		// ------------------------------
		// Increase counter
		// ------------------------------

		for {

			if len(c.nums) == 0 {
				break OUTER
			}

			x := len(c.nums) - 1

			if c.nums[x]+1 < len(c.path[x].edges) {

				c.nums[x]++

				n = c.node()

				if n.isLeaf() {
					c.seek = n.leaf.key
					return n.leaf.key, n.leaf.val
				}

				continue OUTER

			} else {

				c.nums = c.nums[:x]
				c.path = c.path[:x]

				continue

			}

		}

	}

	return nil, nil

}

// Seek moves the cursor to a given key in the tree and returns it.
// If the specified key does not exist then the next key in the tree
// is used. If no keys follow, then a nil key and value are returned.
func (c *Cursor) Seek(key []byte) ([]byte, *List) {

	s := key

	n := c.tree.root

	c.nums = c.nums[:0]
	c.path = c.path[:0]

	var x int

	// OUTER:
	for {

		// Check for key exhaution
		if len(s) == 0 {
			return c.first(n)
		}

		t := n

		// Look for an edge
		if x, n = n.getSub(s[0]); n == nil {

			if len(t.edges) == 0 {
				return c.Next()
			} else if s[0] < t.edges[0].prefix[0] {
				if len(c.path) == 0 {
					return c.first(c.tree.root)
				}
				return c.first(c.path[len(c.path)-1])
			} else if s[0] > t.edges[len(t.edges)-1].prefix[0] {
				if len(c.path) == 0 {
					break
				}
				return c.last(c.path[len(c.path)-1])
			}

			break

		}

		// Consume the search prefix
		if bytes.Compare(s, n.prefix) == 0 {
			c.nums = append(c.nums, x)
			c.path = append(c.path, t)
			s = s[:0]
			continue
		} else if bytes.HasPrefix(s, n.prefix) {
			c.nums = append(c.nums, x)
			c.path = append(c.path, t)
			s = s[len(n.prefix):]
			continue
		} else if bytes.HasPrefix(n.prefix, s) {
			c.nums = append(c.nums, x)
			c.path = append(c.path, t)
			s = s[:0]
			continue
		} else if bytes.Compare(s, n.prefix) < 0 {
			c.nums = append(c.nums, x)
			c.path = append(c.path, t)
			s = s[:0]
			continue
		} else if bytes.Compare(s, n.prefix) > 0 {
			c.nums = append(c.nums, x)
			c.path = append(c.path, t)
			c.last(n)
			return c.Next()
		}

		break

	}

	c.nums = c.nums[:0]
	c.path = c.path[:0]

	return nil, nil

}

// ------

func (c *Cursor) node() *Node {

	var x int

	x = len(c.nums) - 1

	if len(c.path[x].edges) <= c.nums[x] {
		c.Seek(c.seek)
		x = len(c.nums) - 1
	}

	return c.path[x].edges[c.nums[x]]

}

func (c *Cursor) first(n *Node) ([]byte, *List) {

	for {

		if n.isLeaf() {
			c.seek = n.leaf.key
			return n.leaf.key, n.leaf.val
		}

		if len(n.edges) > 0 {
			c.nums = append(c.nums, 0)
			c.path = append(c.path, n)
			n = n.edges[0]
		} else {
			break
		}

	}

	return nil, nil

}

func (c *Cursor) last(n *Node) ([]byte, *List) {

	for {

		if num := len(n.edges); num > 0 {
			c.nums = append(c.nums, num-1)
			c.path = append(c.path, n)
			n = n.edges[num-1]
			continue
		}

		if n.isLeaf() {
			c.seek = n.leaf.key
			return n.leaf.key, n.leaf.val
		}

		break

	}

	return nil, nil

}
