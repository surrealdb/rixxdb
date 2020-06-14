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

package mmap

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestMmap(t *testing.T) {

	const filename = "mmap_test.go"

	r, err := Open(filename)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	data := make([]byte, r.Len())

	if _, err := r.ReadAt(data, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	want, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("ioutil.ReadFile: %v", err)
	}

	if len(data) != len(want) {
		t.Fatalf("Read %d bytes, want %d\n", len(data), len(want))
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("Read %q, want %q\n", string(data), string(want))
	}

}
