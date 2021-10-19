package rixxdb

import (
	"github.com/surrealdb/rixxdb/data"
)

type IT struct {
	ok bool
	cu *data.Cursor
}

func (it *IT) First() ([]byte, *data.List) {
	if k, l := it.cu.First(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Last() ([]byte, *data.List) {
	if k, l := it.cu.Last(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Prev() ([]byte, *data.List) {
	if k, l := it.cu.Prev(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Next() ([]byte, *data.List) {
	if k, l := it.cu.Next(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Seek(key []byte) ([]byte, *data.List) {
	if k, l := it.cu.Seek(key); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Valid() bool {
	return it.ok
}
