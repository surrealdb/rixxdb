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

package rixxdb

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"strconv"

	"github.com/cznic/zappy"
)

var chars = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

// Used to see if we can conditionally put a value. We can only put
// a value if the value is the same, or if both items are nil.
func check(a, b []byte) bool {
	if a != nil && b != nil {
		return bytes.Equal(a, b)
	} else if a == nil && b == nil {
		return true
	}
	return false
}

// Used to see if we can conditionally del a value. We can only del
// a value if the value is the same, and neither item is nil.
func alter(a, b []byte) bool {
	if a != nil && b != nil {
		return bytes.Equal(a, b)
	} else if a == nil && b == nil {
		return false
	}
	return false
}

func encode(src []byte) (dst []byte, err error) {

	if len(src) == 0 {
		return src, nil
	}

	return zappy.Encode(nil, src)

}

func decode(src []byte) (dst []byte, err error) {

	if len(src) == 0 {
		return src, nil
	}

	return zappy.Decode(nil, src)

}

func encrypt(key []byte, src []byte) (dst []byte, err error) {

	if key == nil || len(key) == 0 || len(src) == 0 {
		return src, nil
	}

	// Initiate AES
	block, _ := aes.NewCipher(key)

	// Initiate cipher
	cipher, _ := cipher.NewGCM(block)

	// Initiate nonce
	nonce := random(12)

	dst = cipher.Seal(nil, nonce, src, nil)

	dst = append(nonce[:], dst[:]...)

	return

}

func decrypt(key []byte, src []byte) (dst []byte, err error) {

	if key == nil || len(key) == 0 || len(src) == 0 {
		return src, nil
	}

	// Corrupt
	if len(src) < 12 {
		return src, errors.New("Invalid data")
	}

	// Initiate AES
	block, _ := aes.NewCipher(key)

	// Initiate cipher
	cipher, _ := cipher.NewGCM(block)

	return cipher.Open(nil, src[:12], src[12:], nil)

}

func random(l int) []byte {

	if l == 0 {
		return nil
	}

	i := 0
	t := len(chars)
	m := 255 - (256 % t)
	b := make([]byte, l)
	r := make([]byte, l+(l/4))

	for {

		rand.Read(r)

		for _, rb := range r {
			c := int(rb)
			if c > m {
				continue
			}
			b[i] = chars[c%t]
			i++
			if i == l {
				return b
			}
		}

	}

}

func readver(b *bufio.Reader) (int64, error) {

	data, err := b.ReadBytes(' ')
	if err != nil {
		return 0, err
	}

	data = bytes.TrimSuffix(data, []byte{' '})

	return strconv.ParseInt(string(data), 10, 64)

}

func readkey(b *bufio.Reader) (key []byte, err error) {

	data, err := b.ReadBytes(' ')
	if err != nil {
		return nil, err
	}

	data = bytes.TrimSuffix(data, []byte{' '})

	num, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, err
	}

	if key, err = readall(b, num); err != nil {
		return nil, err
	}

	if _, err = b.Discard(1); err != nil {
		return nil, err
	}

	return

}

func readval(b *bufio.Reader) (val []byte, err error) {

	data, err := b.ReadBytes(' ')
	if err != nil {
		return nil, err
	}

	data = bytes.TrimSuffix(data, []byte{' '})

	num, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, err
	}

	if val, err = readall(b, num); err != nil {
		return nil, err
	}

	if _, err = b.Discard(1); err != nil {
		return nil, err
	}

	return

}

func readall(b *bufio.Reader, num int64) (out []byte, err error) {

	var got int

	out = make([]byte, num)

	for have, want := 0, int(num); have < want; have += got {
		if got, err = b.Read(out[have:]); err != nil {
			return nil, err
		}
	}

	return

}
