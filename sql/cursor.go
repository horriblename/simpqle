package sql

import (
	"github.com/horriblename/simpqle/sql/bptree"
)

type Cursor struct {
	table      *Table
	pageNum    uint64
	cellNum    uint64
	endOfTable bool
}

type tmpTableNode struct {
	node bptree.Node[uint64, Row]
}

func TableStart(table *Table) (*Cursor, error) {
	rootNode, err := table.pager.getPage(table.RootPageNum)
	if err != nil {
		return nil, err
	}

	numCells := rootNode.node.NumCells()

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

	cursor.cellNum += 1
	if cursor.cellNum >= page.node.NumCells() {
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

	leaf, ok := page.node.(*bptree.LeafNode[uint64, Row])
	if !ok {
		panic("TODO: Value on non-leaf")
	}

	// TODO: which value?
	return leaf.LeafNodeCell(0), nil
}

func (cursor *Cursor) leafNodeInsert(key uint64, value *Row) error {
	node, err := cursor.table.pager.getPage(cursor.pageNum)
	if err != nil {
		return err
	}

	numCells := node.node.NumCells()
	if numCells >= bptree.LeafNodeMaxCells {
		// Node full
		panic("unimplemented: leaf node full")
	}

	leaf, ok := node.node.(*bptree.LeafNode[uint64, Row])
	if !ok {
		panic("TODO: handle error")
	}

	leaf.Insert(uint64(cursor.cellNum), key, *value)
	cursor.cellNum += 1

	return node.node.SerializeBinary(cursor.table.pager.file)
}
