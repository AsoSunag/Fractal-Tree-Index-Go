package fti

type array struct {
	startHash  uint64
	endHash    uint64
	nodes      []*node
	DirtyCount uint64
}

func (a *array) findHash(hash uint64) (*node, bool) {
	if hash >= a.startHash && hash <= a.endHash {
		pivot := len(a.nodes) / 2
		for {
			if a.nodes[pivot].Hash > hash {
				pivot = pivot / 2
			} else if a.nodes[pivot].Hash < hash {
				pivot = pivot + pivot/2
			} else {
				return a.nodes[pivot], true
			}

			if a.nodes[pivot].Hash == a.startHash || a.nodes[pivot].Hash == a.endHash {
				if a.nodes[pivot].Hash == hash {
					return a.nodes[pivot], true
				}
				return nil, false
			}

		}
	}
	return nil, false
}
