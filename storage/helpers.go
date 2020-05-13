package storage

import (
	"errors"
	"math/rand"
	"reflect"
	"unsafe"
)

//
//var (
//	ErrInvalidKey = errors.New("corrupted message key should be 8 bytes")
//)

// Message represents a KV but with Message logic
type Message struct {
	Priority uint16
	ID       []byte
	Body     []byte
}

func (m Message) Key() []byte {
	return append(UInt16ToBytes(m.Priority), m.ID...)
}

func NewMessageFromKV(kv KVPair) (Message, error) {
	if len(kv.Key) < 3 {
		return Message{}, errors.New("invalid key")
	}

	priority := getUint16(kv.Key)
	return Message{
		Priority: priority,
		ID:       kv.Key[1:],
		Body:     kv.Val,
	}, nil
}

// KVPair is a simple KeyValue pair
type KVPair struct {
	// For message the  Key is a 64bit (8 bytes) that contains 2 bytes priority + random bytes
	Key []byte
	// For Message is the body
	Val []byte
}

// Clone ensures a copy that do not share the underlying arrays
// It allocates memory!
func (p KVPair) Clone() KVPair {
	result := KVPair{
		Key: make([]byte, len(p.Key)),
		Val: make([]byte, len(p.Val)),
	}

	copy(result.Key, p.Key)
	copy(result.Val, p.Val)
	return result
}

//
//func (m KVPair) GetKeyAsUint64() (uint64, error) {
//	if len(m.Key) != 8 {
//		return -1, ErrInvalidKey
//	}
//	return getUint64(m.Key), nil
//}

func UInt64ToBytes(n uint64) []byte {
	result := make([]byte, 8)
	writeUint64(result, n)
	return result
}

func UInt16ToBytes(partition uint16) []byte {
	buf := make([]byte, 2)
	writeUint16(buf, partition)
	return buf
}
func GenerateMsgKey(priority uint16) []byte {
	priorityPrefix := UInt16ToBytes(priority)
	randomMsgId := make([]byte, 6)
	rand.Read(randomMsgId)
	return append(priorityPrefix, randomMsgId...)
}

// writeUint64 encodes a little-endian uint64 into a byte slice.
func writeUint64(buf []byte, n uint64) {
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

// getUint16 decodes a little-endian uint16 from a byte slice.
func getUint16(buf []byte) (n uint16) {
	_ = buf[1] // Force one bounds check. See: golang.org/issue/14808
	n |= uint16(buf[0])
	n |= uint16(buf[1]) << 8
	return
}

// getUint64 decodes a little-endian uint64 from a byte slice.
func getUint64(buf []byte) (n uint64) {
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

// writeUint16 encodes a little-endian uint16 into a byte slice.
func writeUint16(buf []byte, n uint16) {
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
