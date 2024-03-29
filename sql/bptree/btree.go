// Nodes can have up to m children, where m is called the tree's "order". To
// keep the tree mostly balanced, we also say nodes have to have at least m/2
// children (round up)
//
// Exceptions:
// - Leaf nodes have 0 children
// - The root node can have fewer than m children but must have at least 2
// - If the root node is a leaf node (the only node), it still has 0 children
//
// Nodes with children are called "internal" nodes. Internal nodes and leaf
// nodes are structured differently:
// - Internal Node stores keys and pointers to children
// - Leaf Node stores keys and values
//
// - Internal node stores up to m-1 keys
// - Leaf node stores as many as will fit
//
// - Internal node stores (number of keys + 1) pointers to its children
//
// - Internal node's keys are used for routing
// - Leaf Node's keys are paired with value
//
// - Internal Nodes don't store values
// - Leaf Nodes store values

package bptree

type NodeType int

const (
	Internal NodeType = iota
	Leaf
)

const (
	gNodeNumCells = 3
)

type NodeVariant interface{ node() }
type Node[K comparable, V any] struct {
	IsRoot  bool
	Parent  int64 // page number
	Variant NodeVariant
}

func (_ *LeafNode[K, V]) node()     {}
func (_ *InternalNode[K, V]) node() {}

type LeafNode[K comparable, V any] struct {
	NumCells uint64

	Key   K
	Value V
}

type InternalNode[K comparable, V any] struct {
}

func NewRootNode[K comparable, V any]() Node[K, V] {
	return Node[K, V]{
		IsRoot: true,
		Parent: 0,
		Variant: &LeafNode[K, V]{
			NumCells: gNodeNumCells,
		},
	}
}