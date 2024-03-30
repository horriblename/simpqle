package bptree

import (
	"fmt"
	"strconv"
	"strings"
)

func (leaf LeafNode[K, V]) Visualize() string {
	var buf strings.Builder
	numCells := leaf.NumCells()

	buf.WriteString("leaf (")
	buf.WriteString(strconv.Itoa(int(numCells)))
	buf.WriteString(")\n")

	for _, pair := range leaf.inner.Pairs[:numCells] {
		buf.WriteString(fmt.Sprintf("- %v: %v\n", pair.Key, pair.Value))
	}

	return buf.String()
}

func (leaf InternalNode[K, V]) Visualize() string {
	panic("unimplemented: InternalNode.Visualize()")
}
