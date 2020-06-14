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

// +build windows

package mmap

import (
	"os"
	"syscall"
)

const prot = syscall.PAGE_READONLY
const flag = syscall.FILE_MAP_READ

func mmap(file *os.File, size int) ([]byte, error) {

	low, high := uint32(size), uint32(size>>32)

	hand, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, prot, high, low, nil)
	if err != nil {
		return nil, err
	}

	defer syscall.CloseHandle(hand)

	ptr, err := syscall.MapViewOfFile(hand, flag, 0, 0, uintptr(size))
	if err != nil {
		return nil, err
	}

	return (*[max]byte)(unsafe.Pointer(ptr))[:size], nil

}

func munmap(data []byte) {
	syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
}
