package rixxdb

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
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

func wver(v uint64) (bit []byte) {

	bit = make([]byte, 8)

	binary.BigEndian.PutUint64(bit, uint64(v))

	return
}

func wlen(v []byte) (bit []byte) {

	bit = make([]byte, 8)

	binary.BigEndian.PutUint64(bit, uint64(len(v)))

	return
}

func rbit(r *bufio.Reader) (byte, error) {

	return r.ReadByte()

}

func rint(b *bufio.Reader) (uint64, error) {

	v := make([]byte, 8)

	_, err := io.ReadFull(b, v)

	return binary.BigEndian.Uint64(v), err

}

func rkey(b *bufio.Reader) ([]byte, error) {

	l, err := rint(b)
	if err != nil {
		return nil, err
	}

	v := make([]byte, int(l))

	_, err = io.ReadFull(b, v)

	return v, err

}

func rval(b *bufio.Reader) ([]byte, error) {

	l, err := rint(b)
	if err != nil {
		return nil, err
	}

	v := make([]byte, int(l))

	_, err = io.ReadFull(b, v)

	return v, err

}
