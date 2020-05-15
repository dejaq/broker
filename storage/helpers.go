package storage

import (
	"crypto/rand"
	"io"
)

//
//var (
//	ErrInvalidKey = errors.New("corrupted message key should be 8 bytes")
//)

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

// writeUint64 encodes a little-endian uint64 into a byte slice.
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

// newUUID returns a UUID based on bytes read from crypto/rand.Reader
func newUUID() []byte {
	uuid := make([]byte, 16)
	//nolint:errcheck,gosec //cannot fail
	io.ReadFull(rand.Reader, uuid[:])
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10
	return uuid
}
