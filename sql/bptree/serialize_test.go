package bptree

import (
	"bytes"
	"github.com/go-test/deep"
	"testing"
)

type row struct {
	Name [25]byte
	Age  byte
}

type k uint64
type v row

func TestSerialize(t *testing.T) {
	node := LeafNode[k, v]{innerLeafNode[k, v]{
		IsRoot:   true,
		Parent:   0,
		NumCells: 1,
		Pairs: [LeafNodeMaxCells]KVPair[k, v]{
			{
				Key: 0,
				Value: v{
					Name: [25]byte{'T', 'o', 'm'},
					Age:  12,
				},
			},
		},
	}}

	var buf bytes.Buffer
	err := node.SerializeBinary(&buf)
	assert(err)

	t.Logf("binary: %+x", buf.Bytes())

	got, err := DeserializeBinary[k, v](&buf)
	assert(err)

	if diff := deep.Equal(&node, got); diff != nil {
		t.Error(diff)
	}
}

func assert(err error) {
	if err != nil {
		panic(err)
	}
}
