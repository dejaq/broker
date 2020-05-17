package storage

import (
	"encoding/binary"
)

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

func bytesToSliceUInt16(src []byte) []uint16 {
	var dest []uint16
	for i := 0; i < len(src); i += 2 {
		dest = append(dest, binary.BigEndian.Uint16(src[i:i+2]))
	}
	return dest
}

func sliceUInt16ToBytes(src []uint16) []byte {
	buf := make([]byte, 2)
	result := make([]byte, 0, len(src)*2)

	for _, p := range src {
		binary.BigEndian.PutUint16(buf, p)
		result = append(result, buf...)
	}
	return result
}
