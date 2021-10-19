package rixxdb

// KV represents a KV item stored in the database.
type KV struct {
	ver uint64
	key []byte
	val []byte
}

// Exi returns whether this key-value item actually exists.
func (kv *KV) Exi() bool {
	return kv.val != nil
}

// Key returns the key for the underlying key-value item.
func (kv *KV) Key() []byte {
	return kv.key
}

// Val returns the value for the underlying key-value item.
func (kv *KV) Val() []byte {
	return kv.val
}

// Ver returns the version for the underlying key-value item.
func (kv *KV) Ver() uint64 {
	return kv.ver
}
