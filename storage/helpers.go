package storage

import (
	"errors"
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

// Key prepends the priority to the messageID to be stored in
// a KV store ordered by priority
func (m Message) Key() []byte {
	return append(uInt16ToBytes(m.Priority), m.ID...)
}

// NewMessageFromKV constructs a message from a BadgerKV
// It does not allocate memory
func NewMessageFromKV(kv KVPair) (Message, error) {
	if len(kv.Key) < 3 {
		return Message{}, errors.New("invalid key")
	}

	priority := getUint16(kv.Key)

	return Message{
		Priority: priority,
		ID:       kv.Key[2:],
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
//
//func UInt64ToBytes(n uint64) []byte {
//	result := make([]byte, 8)
//	writeUint64(result, n)
//	return result
//}

func uInt16ToBytes(partition uint16) []byte {
	buf := make([]byte, 2)
	writeUint16(buf, partition)

	return buf
}

//
//func GenerateMsgKey(priority uint16) []byte {
//	priorityPrefix := uInt16ToBytes(priority)
//	randomMsgId := make([]byte, 6)
//	rand.Read(randomMsgId)
//	return append(priorityPrefix, randomMsgId...)
//}
//
//// writeUint64 encodes a little-endian uint64 into a byte slice.
//func writeUint64(buf []byte, n uint64) {
//	_ = buf[7] // Force one bounds check. See: golang.org/issue/14808
//	buf[0] = byte(n)
//	buf[1] = byte(n >> 8)
//	buf[2] = byte(n >> 16)
//	buf[3] = byte(n >> 24)
//	buf[4] = byte(n >> 32)
//	buf[5] = byte(n >> 40)
//	buf[6] = byte(n >> 48)
//	buf[7] = byte(n >> 56)
//}

// getUint16 decodes a little-endian uint16 from a byte slice.
func getUint16(buf []byte) (n uint16) {
	_ = buf[1] // Force one bounds check. See: golang.org/issue/14808
	n |= uint16(buf[0])
	n |= uint16(buf[1]) << 8

	return
}

//
//// getUint64 decodes a little-endian uint64 from a byte slice.
//func getUint64(buf []byte) (n uint64) {
//	_ = buf[7] // Force one bounds check. See: golang.org/issue/14808
//	n |= uint64(buf[0])
//	n |= uint64(buf[1]) << 8
//	n |= uint64(buf[2]) << 16
//	n |= uint64(buf[3]) << 24
//	n |= uint64(buf[4]) << 32
//	n |= uint64(buf[5]) << 40
//	n |= uint64(buf[6]) << 48
//	n |= uint64(buf[7]) << 56
//	return
//}

// writeUint16 encodes a little-endian uint16 into a byte slice.
func writeUint16(buf []byte, n uint16) {
	_ = buf[1] // Force one bounds check. See: golang.org/issue/14808
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
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
