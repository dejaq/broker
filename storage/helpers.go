package storage

import (
	"errors"
	"math/rand"
	"reflect"
	"unsafe"
)

var (
	ErrInvalidKey = errors.New("corrupted message key should be 8 bytes")
)

// This is used for messages but also for other data types
type KVPair struct {
	// For message the  Key is a 64bit (8 bytes) that contains 2 bytes priority + random bytes
	Key []byte
	// For Message is the body
	Val []byte
}

func (m KVPair) GetKeyAsUint64() (uint64, error) {
	if len(m.Key) != 8 {
		return -1, ErrInvalidKey
	}
	return GetUint64(m.Key), nil
}

func UInt64ToBytes(n uint64) []byte {
	result := make([]byte, 8)
	WriteUint64(result, n)
	return result
}

func UInt16ToBytes(partition uint16) []byte {
	buf := make([]byte, 2)
	WriteUint16(buf, partition)
	return buf
}
func generateMsgKey(priority uint16) []byte {
	priorityPrefix := UInt16ToBytes(priority)
	randomMsgId := make([]byte, 6)
	rand.Read(randomMsgId)
	return append(priorityPrefix, randomMsgId...)
}

// WriteUint64 encodes a little-endian uint64 into a byte slice.
func WriteUint64(buf []byte, n uint64) {
	_ = buf[7] // Force one bounds check. See: golang.org/issue/14808
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
	buf[2] = byte(n >> 16)
	buf[3] = byte(n >> 24)
	buf[4] = byte(n >> 32)
	buf[5] = byte(n >> 40)
	buf[6] = byte(n >> 48)
	buf[7] = byte(n >> 56)
}

// GetUint64 decodes a little-endian uint64 from a byte slice.
func GetUint64(buf []byte) (n uint64) {
	_ = buf[7] // Force one bounds check. See: golang.org/issue/14808
	n |= uint64(buf[0])
	n |= uint64(buf[1]) << 8
	n |= uint64(buf[2]) << 16
	n |= uint64(buf[3]) << 24
	n |= uint64(buf[4]) << 32
	n |= uint64(buf[5]) << 40
	n |= uint64(buf[6]) << 48
	n |= uint64(buf[7]) << 56
	return
}

// WriteUint16 encodes a little-endian uint16 into a byte slice.
func WriteUint16(buf []byte, n uint16) {
	_ = buf[1] // Force one bounds check. See: golang.org/issue/14808
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
}

// StringToImmutableBytes returns a slice of bytes from a string without allocating memory
// it is the caller's responsibility  not to mutate the bytes returned.
func StringToImmutableBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}

	// NB(xichen): We need to declare a real byte slice so internally the compiler
	// knows to use an unsafe.Pointer to keep track of the underlying memory so that
	// once the slice's array pointer is updated with the pointer to the string's
	// underlying bytes, the compiler won't prematurely GC the memory when the string
	// goes out of scope.
	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	// NB(xichen): This makes sure that even if GC relocates the string's underlying
	// memory after this assignment, the corresponding unsafe.Pointer in the internal
	// slice struct will be updated accordingly to reflect the memory relocation.
	byteHeader.Data = (*reflect.StringHeader)(unsafe.Pointer(&s)).Data

	// NB(xichen): It is important that we access s after we assign the Data
	// pointer of the string header to the Data pointer of the slice header to
	// make sure the string (and the underlying bytes backing the string) don't get
	// GC'ed before the assignment happens.
	l := len(s)
	byteHeader.Len = l
	byteHeader.Cap = l

	return b
}

func concatSlices(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}
