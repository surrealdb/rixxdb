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
