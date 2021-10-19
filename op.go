package rixxdb

const (
	clr int8 = iota
	del
	put
)

type op struct {
	op  int8
	ver uint64
	key []byte
	val []byte
}
