package utils

import "hash/fnv"

// Hash
func Hash(key []byte) uint32 {
	hash := fnv.New64()
	hash.Write(key)
	return uint32(hash.Sum64())
}
