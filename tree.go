package fti

import (
	"log"
	"math"
	"sync"

	"github.com/OneOfOne/xxhash/native"
)

type Tree struct {
	memoryArrays    []*array
	memoryTmpArrays []*array
	arrayLock       []*sync.Mutex
	initMemArrayLen uint64
}

func (t *Tree) Init(initMemArrayLen uint64) {
	t.initMemArrayLen = initMemArrayLen
	t.memoryArrays = make([]*array, t.initMemArrayLen)
	t.memoryTmpArrays = make([]*array, t.initMemArrayLen)
	t.arrayLock = make([]*sync.Mutex, t.initMemArrayLen)
	var i uint64
	for i = 0; i < initMemArrayLen; i++ {
		t.memoryArrays[i] = &array{
			startHash:  0,
			endHash:    0,
			DirtyCount: 0,
			nodes:      make([]*node, (int)(math.Pow(2, float64(i)))),
		}

		t.memoryTmpArrays[i] = &array{
			startHash:  0,
			endHash:    0,
			DirtyCount: 0,
			nodes:      make([]*node, (int)(math.Pow(2, float64(i)))),
		}
		t.arrayLock[i] = &sync.Mutex{}
	}

	go func() {
		for {
			t.mergeMemoryArrays(t.initMemArrayLen-1, 0)
		}
	}()
}

func (t *Tree) Insert(key string, value []byte) *Error {
	n := &node{
		Hash:  xxhash.ChecksumString64(key),
		Key:   key,
		Value: value,
	}
	t.arrayLock[0].Lock()
	if t.memoryTmpArrays[0].nodes[0] != nil {
		return &Error{
			description: "Cannot insert",
			isTemporary: true,
		}
	}
	t.memoryTmpArrays[0].startHash = n.Hash
	t.memoryTmpArrays[0].endHash = n.Hash
	t.memoryTmpArrays[0].nodes[0] = n
	t.arrayLock[0].Unlock()
	if needGrowth := t.mergeMemoryArrays(0, t.initMemArrayLen); needGrowth {
		t.growMemoryArrays()
	}

	return nil
}

func (t *Tree) Get(key string) ([]byte, *Error) {
	hash := xxhash.ChecksumString64(key)
	for i := 0; i < len(t.memoryArrays); i++ {
		t.arrayLock[i].Lock()
		if node, found := t.memoryArrays[i].findHash(hash); found {
			retval := make([]byte, len(node.Value))
			copy(retval, node.Value)
			t.arrayLock[i].Unlock()
			return retval, nil
		}
		t.arrayLock[i].Unlock()
	}
	return nil, nil
}

func (t *Tree) Delete(key string) *Error {
	hash := xxhash.ChecksumString64(key)
	for i := 0; i < len(t.memoryArrays); i++ {
		t.arrayLock[i].Lock()
		if node, found := t.memoryArrays[i].findHash(hash); found {
			node.Dirty = true
			t.memoryArrays[i].DirtyCount++
			t.arrayLock[i].Unlock()
			return nil
		}
		t.arrayLock[i].Unlock()
	}
	return nil
}

func (t *Tree) Print() {
	for i := 0; i < len(t.memoryArrays); i++ {
		t.arrayLock[i].Lock()
		for _, node := range t.memoryArrays[i].nodes {
			if node != nil {
				log.Println(node)
			}
		}
		t.arrayLock[i].Unlock()
	}
}

func (t *Tree) mergeMemoryArrays(startDepth, endDepth uint64) bool {
	currLen := uint64(len(t.memoryTmpArrays))
	if currLen-1 <= startDepth {
		return false
	} else if endDepth <= startDepth {
		endDepth = currLen - 1
	}
	counter := 0
	for i := startDepth; i < endDepth; i++ {
		t.arrayLock[i].Lock()
		if t.memoryTmpArrays[i].nodes[0] != nil {
			if t.memoryArrays[i].nodes[0] != nil {
				ind1, ind2 := 0, 0
				t.arrayLock[i+1].Lock()
				for j := 0; ind1 < len(t.memoryArrays[i].nodes) || ind2 < len(t.memoryTmpArrays[i].nodes); j++ {
					if ind1 < len(t.memoryArrays[i].nodes) && t.memoryArrays[i].nodes[ind1].Hash < t.memoryTmpArrays[i].nodes[ind2].Hash {
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
				t.memoryTmpArrays[i+1].DirtyCount = t.memoryTmpArrays[i].DirtyCount + t.memoryArrays[i].DirtyCount
				t.memoryTmpArrays[i].DirtyCount = 0
				t.memoryArrays[i].DirtyCount = 0
				t.arrayLock[i+1].Unlock()
			} else {
				t.memoryArrays[i].startHash = t.memoryTmpArrays[i].startHash
				t.memoryArrays[i].endHash = t.memoryTmpArrays[i].endHash
				t.memoryArrays[i].DirtyCount = t.memoryTmpArrays[i].DirtyCount
				t.memoryTmpArrays[i].DirtyCount = 0
				for j, node := range t.memoryTmpArrays[i].nodes {
					t.memoryArrays[i].nodes[j] = node
					t.memoryTmpArrays[i].nodes[j] = nil
				}
			}
		}

		if t.memoryArrays[i].nodes[0] != nil {
			counter++
		}
		t.arrayLock[i].Unlock()
	}
	if counter > len(t.memoryArrays)-1 {
		return true
	}
	return false
}

func (t *Tree) growMemoryArrays() {
	currLen := len(t.memoryArrays)
	t.memoryArrays = append(t.memoryArrays, &array{
		startHash:  0,
		endHash:    0,
		DirtyCount: 0,
		nodes:      make([]*node, (int)(math.Pow(2, float64(currLen)))),
	})

	t.memoryTmpArrays = append(t.memoryTmpArrays, &array{
		startHash:  0,
		endHash:    0,
		DirtyCount: 0,
		nodes:      make([]*node, (int)(math.Pow(2, float64(currLen)))),
	})

	var mut sync.Mutex
	t.arrayLock = append(t.arrayLock, &mut)

}
