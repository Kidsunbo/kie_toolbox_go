package container

import (
	"sync/atomic"
)

type AtomicBitSet struct {
	bits []atomic.Uint64
}

func NewAtomicBitSet(bits int) AtomicBitSet {
	return AtomicBitSet{
		bits: make([]atomic.Uint64, (bits+63)>>6),
	}
}

func (a *AtomicBitSet) Set(bit int) {
	if bit < 0 || bit >= len(a.bits)<<6 {
		panic("bit index out of range")
	}
	a.bits[bit>>6].Or(1 << (bit & 63))
}

func (a *AtomicBitSet) Clear(bit int) {
	if bit < 0 || bit >= len(a.bits)<<6 {
		panic("bit index out of range")
	}
	a.bits[bit>>6].And(^uint64(1 << (bit & 63)))
}

func (a *AtomicBitSet) Get(bit int) bool {
	if bit < 0 || bit >= len(a.bits)<<6 {
		panic("bit index out of range")
	}
	return a.bits[bit>>6].Load()&(1<<(bit&63)) != 0
}

func (a *AtomicBitSet) Any(start int, end int) bool {
	if start < 0 || end < 0 || start >= end {
		panic("invalid range")
	}
	startWord, startBit := start>>6, start&63
	endWord, endBit := (end-1)>>6, (end-1)&63
	if startWord >= len(a.bits) || endWord >= len(a.bits) {
		panic("bit index out of range")
	}

	if startWord == endWord {
		mask := ((uint64(1) << (endBit + 1)) - 1) & ^((uint64(1) << startBit) - 1)
		return a.bits[startWord].Load()&mask != 0
	}

	if a.bits[startWord].Load()&^((uint64(1)<<startBit)-1) != 0 {
		return true
	}
	for i := startWord + 1; i < endWord; i++ {
		if a.bits[i].Load() != 0 {
			return true
		}
	}
	return a.bits[endWord].Load()&((uint64(1)<<(endBit+1))-1) != 0
}

func (a *AtomicBitSet) All(start int, end int) bool {
	if start < 0 || end < 0 || start >= end {
		panic("invalid range")
	}
	startWord, startBit := start>>6, start&63
	endWord, endBit := (end-1)>>6, (end-1)&63
	if startWord >= len(a.bits) || endWord >= len(a.bits) {
		panic("bit index out of range")
	}

	if startWord == endWord {
		mask := ((uint64(1) << (endBit + 1)) - 1) & ^((uint64(1) << startBit) - 1)
		return a.bits[startWord].Load()&mask == mask
	}

	mask := ^((uint64(1) << startBit) - 1)
	if a.bits[startWord].Load()&mask != mask {
		return false
	}
	for i := startWord + 1; i < endWord; i++ {
		if a.bits[i].Load() != ^uint64(0) {
			return false
		}
	}
	mask = (uint64(1) << (endBit + 1)) - 1
	return a.bits[endWord].Load()&mask == mask
}
