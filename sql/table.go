package sql

import (
	"errors"
	"fmt"

	"github.com/horriblename/simpqle/sql/bptree"
)

type Table struct {
	pager       *Pager
	RootPageNum uint64
}

var (
	ErrTableFull          = errors.New("table is full")
	ErrInsertDuplicateKey = errors.New("attempt to insert duplicate key")
)

func (table *Table) ExecuteInsert(stmt *Stmt) error {
	// FIXME: single page tree
	page, err := table.pager.getPage(table.RootPageNum)
	if err != nil {
		return err
	}

	if page.node.NumCells() >= bptree.LeafNodeMaxCells {
		return ErrTableFull
	}

	rowToInsert := stmt.RowToInsert
	keyToInsert := rowToInsert.Id
	cursor, err := TableFind(table, keyToInsert)
	if err != nil {
		return err
	}

	if cursor.cellNum < page.node.NumCells() {
		leaf, ok := page.node.(*bptree.LeafNode[uint64, Row])
		if !ok {
			panic("TODO: handle non leaf node")
		}

		keyAtIndex := leaf.KeyAtCell(int(cursor.cellNum))
		if keyAtIndex == uint64(keyToInsert) {
			return ErrInsertDuplicateKey
		}
	}

	err = cursor.leafNodeInsert(uint64(rowToInsert.Id), rowToInsert)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) ExecuteSelect(stmt *Stmt) error {
	cursor, err := TableStart(t)
	if err != nil {
		return err
	}

	for !cursor.endOfTable {
		row, err := cursor.Value()
		if err != nil {
			return err
		}
		printRow(row)
		if err := cursor.Advance(); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) ExecuteStmt(stmt *Stmt) error {
	switch stmt.Kind {
	case Insert:
		return t.ExecuteInsert(stmt)
	case Select:
		return t.ExecuteSelect(stmt)
	default:
		return fmt.Errorf("invalid Stmt.Kind: %d", stmt.Kind)
	}
}

func DbOpen(fname string) (*Table, error) {
	pager, err := pagerOpen(fname)
	if err != nil {
		return nil, err
	}

	table := &Table{}
	table.pager = pager
	table.RootPageNum = tmpRootPageNum

	if pager.numPages == 0 {
		// New database file. Initialize page 0 as leaf node
		rootNode, err := pager.getPage(0)
		if err != nil {
			return nil, err
		}
		rootNode.node = bptree.NewRootNode[uint64, Row]()
	}

	return table, nil
}

// 1. flushes the page cache to disk
// 2. closes the database file
// 3. frees the memory for the Pager and Table data structure (if we're in C lmao)
func (table *Table) Close() error {
	pager := table.pager

	for i, page := range pager.pages {
		if page == nil {
			continue
		}

		pager.flush(uint64(i))
	}

	if err := pager.file.Close(); err != nil {
		return fmt.Errorf("closing db file: %w", err)
	}

	// we don't need these since we have GC, but eh
	for i, page := range pager.pages {
		if page != nil {
			pager.pages[i] = nil
		}
	}
	pager = nil
	return nil
}

func (table *Table) Find(key int64) *Cursor {
	panic("TODO")
}
