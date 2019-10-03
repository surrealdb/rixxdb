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

// Tree represents an immutable versioned radix tree.
type Tree struct {
	size int
	root *Node
}

// New returns an empty Tree
func New() *Tree {
	return &Tree{root: &Node{}}
}

// Size is used to return the number of elements in the tree.
func (t *Tree) Size() int {
	return t.size
}

// Copy starts a new transaction that can be used to mutate the tree
func (t *Tree) Copy() *Copy {
	return &Copy{size: t.size, root: t.root}
}

// Walker represents a callback function which is to be used when
// iterating through the tree using Path, Subs, or Walk. It will be
// populated with the key and list of the current item, and returns
// a bool signifying if the iteration should be terminated.
type Walker func(key []byte, val *List) (exit bool)
