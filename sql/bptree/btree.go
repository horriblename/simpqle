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

import "io"

type NodeType int

const (
	Internal NodeType = iota
	Leaf
)

const (
	LeafNodeMaxCells = 3
)

type Node[K comparable, V any] interface {
	IsRoot() bool
	Parent() int64 // page number
	NumCells() uint64

	SerializeBinary(w io.Writer) error
}

func (leaf *LeafNode[K, V]) IsRoot() bool      { return leaf.inner.IsRoot }
func (leaf *LeafNode[K, V]) Parent() int64     { return leaf.inner.Parent }
func (leaf *LeafNode[K, V]) NumCells() uint64  { return leaf.inner.NumCells }
func (_ *InternalNode[K, V]) IsRoot() bool     { panic("unimplemented") }
func (_ *InternalNode[K, V]) Parent() int64    { panic("unimplemented") }
func (_ *InternalNode[K, V]) NumCells() uint64 { panic("unimplemented") }

type KVPair[K any, V any] struct {
	Key   K
	Value V
}

type LeafNode[K comparable, V any] struct {
	inner innerLeafNode[K, V]
}

type innerLeafNode[K comparable, V any] struct {
	IsRoot   bool
	Parent   int64
	NumCells uint64

	Pairs [LeafNodeMaxCells]KVPair[K, V]
}

type InternalNode[K comparable, V any] struct {
	inner innerInternalNode[K, V]
}

type innerInternalNode[K comparable, V any] struct{}

func NewRootNode[K comparable, V any]() Node[K, V] {
	return &LeafNode[K, V]{innerLeafNode[K, V]{
		IsRoot:   true,
		Parent:   0,
		NumCells: 0,
		Pairs:    [LeafNodeMaxCells]KVPair[K, V]{},
	}}
}

func (leaf *LeafNode[K, V]) LeafNodeCell(cellNum int) *V {
	if len(leaf.inner.Pairs) <= cellNum {
		panic("cellNum out of range")
	}

	return &leaf.inner.Pairs[cellNum].Value
}

func (leaf *LeafNode[K, V]) Insert(cellNum uint64, key K, value V) error {
	if cellNum >= LeafNodeMaxCells {
		// TODO:
		panic("insert out of range: cellNum >= LeafNodeMaxCells")
	}

	cell := KVPair[K, V]{
		Key:   key,
		Value: value,
	}

	if cellNum < leaf.inner.NumCells {
		for i := leaf.inner.NumCells; i > cellNum; i-- {
			leaf.inner.Pairs[i] = leaf.inner.Pairs[i-1]
		}
		leaf.inner.Pairs[cellNum] = cell
		leaf.inner.NumCells += 1
		return nil
	}

	leaf.inner.Pairs[cellNum] = cell
	leaf.inner.NumCells += 1
	return nil
}
