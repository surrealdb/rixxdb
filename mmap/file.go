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

// +build !linux,!darwin,!windows

package mmap

import (
	"errors"
	"os"
)

// Mapper is a memory-mapped file.
type Mapper struct {
	file *os.File
	size int
}

// Open memory-maps the named file for reading.
func Open(path string) (*Mapper, error) {

	// Open the file for mapping
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	// Get the file details
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Get the size of the file
	size := int(stat.Size())

	m := &Mapper{
		file: file,
		size: size,
	}

	return m, nil

}

func (m *Mapper) Len() int {
	return m.size
}

func (m *Mapper) Grow() (err error) {

	var stat os.FileInfo

	// Get the file details
	stat, err = m.file.Stat()
	if err != nil {
		return
	}

	// Get the size of the file
	m.size = int(stat.Size())

	return

}

func (m *Mapper) Close() (err error) {

	if m.file == nil {
		return nil
	}

	return m.file.Close()

}

func (m *Mapper) Seek(off int64, whence int) (int64, error) {

	if m.file == nil {
		return 0, errors.New("Mapper closed")
	}

	return m.file.Seek(off, whence)

}

func (m *Mapper) Read(p []byte) (n int, err error) {

	if m.file == nil {
		return 0, errors.New("Mapper closed")
	}

	return m.file.Read(p)

}

func (m *Mapper) ReadByte() (byte, error) {

	if m.file == nil {
		return 0, errors.New("Mapper closed")
	}

	p := make([]byte, 1)

	n, err := m.file.Read(p)
	if n != len(p) {
		return 0, io.EOF
	}

	return p[0], err

}

func (m *Mapper) ReadAt(p []byte, off int64) (n int, err error) {

	if m.file == nil {
		return 0, errors.New("Mapper closed")
	}

	if off < 0 {
		return 0, errors.New("Invalid ReadAt offset")
	}

	if off > int64(m.size) {
		return 0, errors.New("Invalid ReadAt offset")
	}

	return m.file.ReadAt(p, off)

}
