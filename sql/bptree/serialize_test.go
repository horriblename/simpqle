package bptree

import (
	"bytes"
	"reflect"
	"testing"
)

type row struct {
	Name [25]byte
	Age  byte
}

type k uint64
type v row

func TestSerialize(t *testing.T) {
	node := Node[k, v]{
		IsRoot: true,
		Parent: 0,
		Variant: &LeafNode[k, v]{
			NumCells: 0,
			Pairs: [LeafNodeMaxCells]KVPair[k, v]{
				{
					Key: 0,
					Value: v{
						Name: [25]byte{'T', 'o', 'm'},
						Age:  12,
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	err := node.SerializeBinary(&buf)
	assert(err)

	got, err := DeserializeBinary[k, v](&buf)
	assert(err)

	if !reflect.DeepEqual(node, got) {
		t.Errorf(
			"serialized data does not deserialize to same data, node:\n  %#v\nVariant:\n %#+v\nExpected variant:\n %#v\n",
			got, got.Variant, node.Variant)
	}
}

func assert(err error) {
	if err != nil {
		panic(err)
	}
}
