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

package rixxdb

import (
	"github.com/abcum/rixxdb/data"
)

type IT struct {
	ok bool
	cu *data.Cursor
}

func (it *IT) First() ([]byte, *data.Trie) {
	if k, l := it.cu.First(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Last() ([]byte, *data.Trie) {
	if k, l := it.cu.Last(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Prev() ([]byte, *data.Trie) {
	if k, l := it.cu.Prev(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Next() ([]byte, *data.Trie) {
	if k, l := it.cu.Next(); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Seek(key []byte) ([]byte, *data.Trie) {
	if k, l := it.cu.Seek(key); k != nil {
		return k, l
	}
	it.ok = false
	return nil, nil
}

func (it *IT) Valid() bool {
	return it.ok
}
