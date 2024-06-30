package hedge

import "github.com/cespare/xxhash"

type hashT struct{}

func (h hashT) Sum64(data []byte) uint64 { return xxhash.Sum64(data) }
