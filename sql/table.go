package sql

import (
	"errors"
	"fmt"
	"unsafe"
)

type Table struct {
	NumRows uint64
	pager   *Pager
}

var ErrTableFull = errors.New("table is full")

func rowSlot(table *Table, rowNum uint64) (unsafe.Pointer, error) {
	pageNum := rowNum / gRowsPerPage
	page, err := table.pager.getPage(pageNum)
	if err != nil {
		return nil, err
	}

	rowOffset := rowNum % gRowsPerPage
	byteOffset := rowOffset * gRowSize
	return unsafe.Pointer(uintptr(unsafe.Pointer(page)) + uintptr(byteOffset)), nil
}

func (table *Table) ExecuteInsert(stmt *Stmt) error {
	if table.NumRows >= gTableMaxRows {
		return ErrTableFull
	}

	rowToInsert := stmt.RowToInsert

	rowPtr, err := rowSlot(table, table.NumRows)
	if err != nil {
		return err
	}
	*(*Row)(rowPtr) = *rowToInsert

	table.NumRows += 1

	return nil
}

func (t *Table) ExecuteSelect(stmt *Stmt) error {
	for i := uint64(0); i < t.NumRows; i++ {
		row, err := rowSlot(t, i)
		if err != nil {
			return err
		}
		printRow((*Row)(row))
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
	numRows := pager.fileLen / gRowSize

	table := &Table{}
	table.pager = pager
	table.NumRows = numRows

	return table, nil
}

// 1. flushes the page cache to disk
// 2. closes the database file
// 3. frees the memory for the Pager and Table data structure (if we're in C lmao)
func (table *Table) Close() error {
	pager := table.pager
	numFullPages := table.NumRows / gRowsPerPage

	for i, page := range pager.pages {
		if page == nil {
			continue
		}

		pager.flush(uint64(i), gPageSize)
	}

	// There may be a partial page to write to the end of the file
	// This should not be needed after we switch to a B-tree
	numAdditionalRows := table.NumRows % gRowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pager.flush(pageNum, numAdditionalRows*gRowSize)
			pager.pages[pageNum] = nil
		}
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
	table = nil
	return nil
}
