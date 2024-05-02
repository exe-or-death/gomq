package curve

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"golang.org/x/crypto/curve25519"
)

type Nonce [24]byte

func (n *Nonce) Short(prefix string, nonce uint64) {
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), 16)
	asSlice := unsafe.Slice((*byte)(unsafe.Pointer(n)), 24)
	copy(asSlice[:16], prefixBytes)
	binary.BigEndian.AppendUint64(asSlice[16:16], nonce)
}

func (n *Nonce) Long(prefix string) {
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), 8)
	asSlice := unsafe.Slice((*byte)(unsafe.Pointer(n)), 24)
	copy(asSlice[:8], prefixBytes)
	if _, err := rand.Reader.Read(asSlice[8:]); err != nil {
		panic(fmt.Errorf("Failed creating long nonce: %w", err))
	}
}

func (n *Nonce) FromLong(prefix string, long []byte) {
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), 8)
	asSlice := unsafe.Slice((*byte)(unsafe.Pointer(n)), 24)
	copy(asSlice[:8], prefixBytes)
	copy(asSlice[8:], long)
}

func (n *Nonce) N() *[24]byte {
	return (*[24]byte)(n)
}

func GenerateKeys(pub, sec *[32]byte) {
	PopulateSecKey(sec)
	curve25519.ScalarBaseMult(pub, sec)
}

func PopulateSecKey(sec *[32]byte) {
	_, err := io.ReadFull(rand.Reader, sec[:])
	if err != nil {
		panic(err)
	}
}

func isEmpty(key *[32]byte) bool {
	for _, b := range key {
		if b != 0 {
			return false
		}
	}

	return true
}
