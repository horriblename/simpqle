package sql

import (
	"errors"

	"github.com/horriblename/simpqle/sql/bptree"
)

type Cursor struct {
	table      *Table
	pageNum    uint64
	cellNum    uint64
	endOfTable bool
}

var (
	errWrongVariant error = errors.New("wrong variant")
)

func assert(err error) {
	if err != nil {
		panic(err)
	}
}

type tmpTableNode struct {
	node bptree.Node[uint64, Row]
}
type tmpLeaf bptree.LeafNode[uint64, Row]
type tmpRoot bptree.InternalNode[uint64, Row]
type tmpKey uint64
type tmpVal Row

func TableStart(table *Table) (*Cursor, error) {
	rootNode, err := table.pager.getPage(table.RootPageNum)
	if err != nil {
		return nil, err
	}

	leaf, err := asLeaf(rootNode.node.Variant)
	if err != nil {
		return nil, err
	}

	numCells := leaf.NumCells

	return &Cursor{
		table:      table,
		pageNum:    table.RootPageNum,
		cellNum:    numCells,
		endOfTable: numCells == 0,
	}, nil

}

func TableEnd(table *Table) *Cursor {
	return &Cursor{
		table:      table,
		pageNum:    table.RootPageNum,
		endOfTable: true,
	}
}

func (cursor *Cursor) Advance() error {
	pageNum := cursor.pageNum
	page, err := cursor.table.pager.getPage(pageNum)
	if err != nil {
		return err
	}
	leaf, err := asLeaf(page.node.Variant)
	if err != nil {
		return err
	}

	cursor.cellNum += 1
	if cursor.cellNum >= leaf.NumCells {
		cursor.endOfTable = true
	}

	return nil
}

func (cursor *Cursor) Value() (*Row, error) {
	pageNum := cursor.pageNum
	page, err := cursor.table.pager.getPage(pageNum)
	if err != nil {
		return nil, err
	}

	leaf, err := asLeaf(page.node.Variant)
	if err != nil {
		return nil, err
	}

	return (*Row)(&leaf.Value), nil
}

func asLeaf(variant bptree.NodeVariant) (*bptree.LeafNode[uint64, Row], error) {
	if variant == nil {
		return nil, bptree.ErrNilVariant
	}

	if leaf, ok := variant.(*bptree.LeafNode[uint64, Row]); ok {
		return leaf, nil
	}

	return nil, errWrongVariant
}

func panicCast[T any](x any) *T {
	if t, ok := x.(*T); ok {
		return t
	} else {
		panic("bad cast")
	}
}
