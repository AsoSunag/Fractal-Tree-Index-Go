package fractaltreeindex

import (
	"errors"
	"github.com/OneOfOne/xxhash/native"
	"math"
)

type Tree struct {
	memoryArrays    [][]*node
	memoryTmpArrays [][]*node
}

func (t *Tree) Init() {
	// For now no config is provided so settings are hardcoded
	t.memoryArrays = make([][]*node, 16)
	t.memoryTmpArrays = make([][]*node, 16)
	for i := 0; i < 16; i++ {
		t.memoryArrays[i] = make([]*node, 0, int(math.Pow(2, float64(i))))
		t.memoryTmpArrays[i] = make([]*node, 0, int(math.Pow(2, float64(i))))
	}
	go t.mergeArrays()
}

func (t *Tree) Insert(key string, value []byte) error {
	n := &node{
		Hash:  xxhash.ChecksumString64(key),
		Key:   key,
		Value: value,
	}
	if t.memoryTmpArrays[0][0] != nil {
		return errors.New("Cannot insert temporary error")
	}
	t.memoryTmpArrays[0][0] = n
	return nil
}

func (t *Tree) mergeArrays() {
	for i := 0; i < 15; i++ {
		if t.memoryTmpArrays[i][0] != nil {
			if t.memoryArrays[i][0] != nil {
				ind1, ind2 := 0, 0
				for j := 0; ind1 < len(t.memoryArrays[i]) && ind2 < len(t.memoryTmpArrays[i]); j++ {
					if t.memoryArrays[i][ind1].Hash < t.memoryTmpArrays[i][ind2].Hash {
						t.memoryArrays[i+1][j] = t.memoryArrays[i][ind1]
						t.memoryArrays[i][ind1] = nil
						ind1++
					} else {
						t.memoryArrays[i+1][j] = t.memoryTmpArrays[i][ind2]
						t.memoryTmpArrays[i][ind2] = nil
						ind2++
					}
				}
			}
		}
	}
}
