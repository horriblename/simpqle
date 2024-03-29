package bptree

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	ErrNilVariant     error = errors.New("node variant is nil")
	ErrUnknownVariant error = errors.New("node variant has unknown type")

	ErrUnknownVariantTag error = errors.New("found unrecognized variant tag during decoding")
)

type variantTag uint8

const (
	leafVariant variantTag = iota
	internalVariant
)

func (node *Node[K, V]) SerializeBinary(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, node.IsRoot)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, node.Parent)
	if err != nil {
		return err
	}

	switch variant := node.Variant.(type) {
	case *LeafNode[K, V]:
		err = binary.Write(w, binary.BigEndian, leafVariant)
		if err != nil {
			return err
		}

		err = binary.Write(w, binary.BigEndian, variant)
		if err != nil {
			return err
		}

	case *InternalNode[K, V]:
		panic("unimplemented: serialize internal node")

	case nil:
		return ErrNilVariant

	default:
		return ErrUnknownVariant
	}

	return nil
}

func DeserializeBinary[K comparable, V any](r io.Reader) (Node[K, V], error) {
	node := Node[K, V]{}
	err := binary.Read(r, binary.BigEndian, &node.IsRoot)
	if err != nil {
		return node, err
	}

	err = binary.Read(r, binary.BigEndian, &node.Parent)
	if err != nil {
		return node, err
	}

	var variant variantTag
	err = binary.Read(r, binary.BigEndian, &variant)
	if err != nil {
		return node, err
	}

	switch variant {
	case leafVariant:
		var leaf LeafNode[K, V]
		err = binary.Read(r, binary.BigEndian, &leaf)
		if err != nil {
			return node, err
		}

		node.Variant = &leaf

	case internalVariant:
		panic("unimplemented: deserialize internal node")

	default:
		return node, ErrUnknownVariantTag
	}

	return node, nil
}
