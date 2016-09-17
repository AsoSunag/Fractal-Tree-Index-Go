package fti

import (
	"errors"
	"github.com/OneOfOne/xxhash/native"
	"math"
	"sync"
)

type Tree struct {
	memoryArrays    []*array
	memoryTmpArrays []*array
	arrayLock       []sync.Mutex
}

func (t *Tree) Init() {
	// For now no config is provided so settings are hardcoded
	t.memoryArrays = make([]*array, 16)
	t.memoryTmpArrays = make([]*array, 16)
	t.arrayLock = make([]sync.Mutex, 16)
	for i := 0; i < 16; i++ {
		t.memoryArrays[i] = &array{
			startHash: 0,
			endHash:   0,
			nodes:     make([]*node, (int)(math.Pow(2, float64(i)))),
		}

		t.memoryTmpArrays[i] = &array{
			startHash: 0,
			endHash:   0,
			nodes:     make([]*node, (int)(math.Pow(2, float64(i)))),
		}
	}
	go t.mergeArrays()
}

func (t *Tree) Insert(key string, value []byte) error {
	n := &node{
		Hash:  xxhash.ChecksumString64(key),
		Key:   key,
		Value: value,
	}
	t.arrayLock[0].Lock()
	defer t.arrayLock[0].Unlock()
	if t.memoryTmpArrays[0].nodes[0] != nil {
		return errors.New("Cannot insert temporary error")
	}
	t.memoryTmpArrays[0].startHash = n.Hash
	t.memoryTmpArrays[0].endHash = n.Hash
	t.memoryTmpArrays[0].nodes[0] = n

	return nil
}

func (t *Tree) Get(key string) ([]byte, error) {
	hash := xxhash.ChecksumString64(key)
	for i := 0; i < 16; i++ {
		t.arrayLock[i].Lock()
		if val, found := t.memoryArrays[i].findHash(hash); found {
			return val, nil
		}
		t.arrayLock[i].Unlock()
	}
	return nil, nil
}

func (t *Tree) mergeArrays() {
	for {
		for i := 0; i < 16; i++ {
			t.arrayLock[i].Lock()
			if t.memoryTmpArrays[i].nodes[0] != nil {
				if t.memoryArrays[i].nodes[0] != nil {
					ind1, ind2 := 0, 0
					t.arrayLock[i+1].Lock()
					for j := 0; ind1 < len(t.memoryArrays[i].nodes) && ind2 < len(t.memoryTmpArrays[i].nodes); j++ {
						if t.memoryArrays[i].nodes[ind1].Hash < t.memoryTmpArrays[i].nodes[ind2].Hash {
							t.memoryTmpArrays[i+1].nodes[j] = t.memoryArrays[i].nodes[ind1]
							t.memoryArrays[i].nodes[ind1] = nil
							ind1++
						} else {
							t.memoryTmpArrays[i+1].nodes[j] = t.memoryTmpArrays[i].nodes[ind2]
							t.memoryTmpArrays[i].nodes[ind2] = nil
							ind2++
						}
						if j == 0 {
							t.memoryTmpArrays[i+1].startHash = t.memoryTmpArrays[i+1].nodes[0].Hash
						} else if ind1 == len(t.memoryArrays[i].nodes) && ind2 == len(t.memoryTmpArrays[i].nodes) {
							t.memoryTmpArrays[i+1].endHash = t.memoryTmpArrays[i+1].nodes[j].Hash
						}
					}
					t.arrayLock[i+1].Unlock()
				} else {
					t.memoryArrays[i].startHash = t.memoryTmpArrays[i].startHash
					t.memoryArrays[i].endHash = t.memoryTmpArrays[i].endHash
					for j, node := range t.memoryTmpArrays[i].nodes {
						t.memoryArrays[i].nodes[j] = node
						t.memoryTmpArrays[i].nodes[j] = nil

					}
				}
			}
			t.arrayLock[i].Unlock()
		}
	}
}
