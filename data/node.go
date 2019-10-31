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
	"sort"
)

// Node represents an immutable node in the radix tree which
// can be either an edge node or a leaf node.
type Node struct {
	leaf   *leaf
	edges  []*Node
	prefix []byte
}

type leaf struct {
	key []byte
	val *Trie
}

// Min returns the key and value of the minimum item in the
// subtree of the current node.
func (n *Node) Min() ([]byte, *Trie) {

	for {

		if n.isLeaf() {
			return n.leaf.key, n.leaf.val
		}

		if len(n.edges) > 0 {
			n = n.edges[0]
		} else {
			break
		}

	}

	return nil, nil

}

// Max returns the key and value of the maximum item in the
// subtree of the current node.
func (n *Node) Max() ([]byte, *Trie) {

	for {

		if num := len(n.edges); num > 0 {
			n = n.edges[num-1]
			continue
		}

		if n.isLeaf() {
			return n.leaf.key, n.leaf.val
		}

		break

	}

	return nil, nil

}

// Path is used to recurse over the tree only visiting nodes
// which are above this node in the tree.
func (n *Node) Path(k []byte, f Walker) {

	s := k

	for {

		if n.leaf != nil {
			if f(n.leaf.key, n.leaf.val) {
				return
			}
		}

		if len(s) == 0 {
			return
		}

		if _, n = n.getSub(s[0]); n == nil {
			return
		}

		if bytes.HasPrefix(s, n.prefix) {
			s = s[len(n.prefix):]
		} else {
			break
		}

	}

}

// Subs is used to recurse over the tree only visiting nodes
// which are directly under this node in the tree.
func (n *Node) Subs(k []byte, f Walker) {

	s := k

	for {

		// Check for key exhaution
		if len(s) == 0 {
			subs(n, f, false)
			return
		}

		// Look for an edge
		if _, n = n.getSub(s[0]); n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(s, n.prefix) {
			s = s[len(n.prefix):]
		} else if bytes.HasPrefix(n.prefix, s) {
			subs(n, f, true)
			return
		} else {
			break
		}

	}

}

// Walk is used to recurse over the tree only visiting nodes
// which are under this node in the tree.
func (n *Node) Walk(k []byte, f Walker) {

	s := k

	for {

		// Check for key exhaution
		if len(s) == 0 {
			walk(n, f, false)
			return
		}

		// Look for an edge
		if _, n = n.getSub(s[0]); n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(s, n.prefix) {
			s = s[len(n.prefix):]
		} else if bytes.HasPrefix(n.prefix, s) {
			walk(n, f, false)
			return
		} else {
			break
		}

	}

}

// ------------------------------
// ------------------------------
// ------------------------------
// ------------------------------
// ------------------------------

func (n *Node) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node) dup() *Node {
	d := &Node{}
	if n.leaf != nil {
		d.leaf = &leaf{}
		*d.leaf = *n.leaf
	}
	if n.prefix != nil {
		d.prefix = make([]byte, len(n.prefix))
		copy(d.prefix, n.prefix)
	}
	if len(n.edges) != 0 {
		d.edges = make([]*Node, len(n.edges))
		copy(d.edges, n.edges)
	}
	return d
}

func (n *Node) addSub(s *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].prefix[0] >= s.prefix[0]
	})
	n.edges = append(n.edges, s)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = s
	}
}

func (n *Node) repSub(s *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].prefix[0] >= s.prefix[0]
	})
	if idx < num && n.edges[idx].prefix[0] == s.prefix[0] {
		n.edges[idx] = s
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getSub(label byte) (int, *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].prefix[0] >= label
	})
	if idx < num && n.edges[idx].prefix[0] == label {
		return idx, n.edges[idx]
	}
	return -1, nil
}

func (n *Node) delSub(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].prefix[0] >= label
	})
	if idx < num && n.edges[idx].prefix[0] == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = nil
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *Node) mergeChild() {
	e := n.edges[0]
	child := e
	n.prefix = concat(n.prefix, child.prefix)
	if child.leaf != nil {
		n.leaf = new(leaf)
		*n.leaf = *child.leaf
	} else {
		n.leaf = nil
	}
	if len(child.edges) != 0 {
		n.edges = make([]*Node, len(child.edges))
		copy(n.edges, child.edges)
	} else {
		n.edges = nil
	}
}

func subs(n *Node, f Walker, sub bool) bool {

	// Visit the leaf values if any
	if sub && n.leaf != nil {
		if f(n.leaf.key, n.leaf.val) {
			return true
		}
		return false
	}

	// Recurse on the children
	for _, e := range n.edges {
		if subs(e, f, true) {
			return true
		}
	}

	return false

}

func walk(n *Node, f Walker, sub bool) bool {

	// Visit the leaf values if any
	if n.leaf != nil {
		if f(n.leaf.key, n.leaf.val) {
			return true
		}
	}

	// Recurse on the children
	for _, e := range n.edges {
		if walk(e, f, true) {
			return true
		}
	}

	return false

}

func (n *Node) get(k []byte) *Trie {

	s := k

	for {

		// Check for key exhaution
		if len(s) == 0 {
			if n.isLeaf() {
				return n.leaf.val
			}
			break
		}

		// Look for an edge
		_, n = n.getSub(s[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(s, n.prefix) {
			s = s[len(n.prefix):]
		} else {
			break
		}

	}

	return nil

}
