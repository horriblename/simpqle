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

func (node *LeafNode[K, V]) SerializeBinary(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, leafVariant)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, node.inner)
}

func (node *InternalNode[K, V]) SerializeBinary(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, internalVariant)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, node)
}

func DeserializeBinary[K comparable, V any](r io.Reader) (Node[K, V], error) {
	var variant variantTag
	err := binary.Read(r, binary.BigEndian, &variant)
	if err != nil {
		return nil, err
	}

	switch variant {
	case leafVariant:
		var leaf LeafNode[K, V]
		err = binary.Read(r, binary.BigEndian, &leaf.inner)
		if err != nil {
			return nil, err
		}

		return &leaf, nil

	case internalVariant:
		var internalNode InternalNode[K, V]
		err = binary.Read(r, binary.BigEndian, &internalNode.inner)
		if err != nil {
			return nil, err
		}

		return &internalNode, nil

	default:
		return nil, ErrUnknownVariantTag
	}
}
