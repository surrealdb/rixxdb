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

// +build linux darwin

package mmap

import (
	"errors"
	"io"
	"os"
	"runtime"
)

// Mapper is a memory-mapped file.
type Mapper struct {
	file *os.File
	pos  int    // Position of reader
	size int    // Size of the file
	open int    // Size of the mmap
	data []byte // Memory mapped data
}

func extend(size int) int {
	n := size * 2
	if n < 104857600 {
		return 104857600
	} else {
		return n
	}
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

	// Get the size to be mmapped
	open := extend(size)

	// Get the memory mapped slice
	data, err := mmap(file, open)
	if err != nil {
		return nil, err
	}

	m := &Mapper{
		file: file,
		size: size,
		open: open,
		data: data,
	}

	runtime.SetFinalizer(m, (*Mapper).Close)

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

	// Check if mapping needs growing
	if m.size < m.open {
		return
	}

	// Get the size to be mmapped
	m.open = extend(m.size)

	// Get the memory mapped slice
	m.data, err = mmap(m.file, m.open)
	if err != nil {
		return
	}

	return

}

func (m *Mapper) Close() (err error) {

	if m.file == nil {
		return nil
	}

	m.file.Close()

	if m.data == nil {
		return nil
	}

	data := m.data
	m.data = nil

	runtime.SetFinalizer(m, nil)

	return munmap(data)

}

func (m *Mapper) Seek(off int64, whence int) (int64, error) {

	if m.data == nil {
		return 0, errors.New("Mapper closed")
	}

	if off < 0 {
		return 0, errors.New("Invalid Seek offset")
	}

	if off > int64(m.size) {
		return 0, errors.New("Invalid Seek offset")
	}

	if whence == 1 && m.pos+int(off) > m.size {
		return 0, errors.New("Invalid Seek offset")
	}

	switch whence {
	case 0: // beg
		m.pos = int(off)
	case 1: // current
		m.pos = m.pos + int(off)
	case 2: // end
		m.pos = m.size - int(off)
	}

	return int64(m.pos), nil

}

func (m *Mapper) Read(p []byte) (n int, err error) {

	if m.data == nil {
		return 0, errors.New("Mapper closed")
	}

	n = copy(p, m.data[m.pos:m.size])

	m.pos += n

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil

}

func (m *Mapper) ReadByte() (byte, error) {

	if m.data == nil {
		return 0, errors.New("Mapper closed")
	}

	if m.pos == m.size {
		return 0, io.EOF
	}

	b := m.data[m.pos]

	m.pos++

	return b, nil

}

func (m *Mapper) ReadAt(p []byte, off int64) (n int, err error) {

	if m.data == nil {
		return 0, errors.New("Mapper closed")
	}

	if off < 0 {
		return 0, errors.New("Invalid ReadAt offset")
	}

	if off > int64(m.size) {
		return 0, errors.New("Invalid ReadAt offset")
	}

	n = copy(p, m.data[off:m.size])

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil

}
