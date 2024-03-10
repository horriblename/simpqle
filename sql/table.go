package sql

import (
	"errors"
	"fmt"
)

type Table struct {
	NumRows uint64
	pager   *Pager
}

var ErrTableFull = errors.New("table is full")

func (table *Table) ExecuteInsert(stmt *Stmt) error {
	if table.NumRows >= gTableMaxRows {
		return ErrTableFull
	}

	rowToInsert := stmt.RowToInsert
	cursor := TableEnd(table)

	row, err := cursor.Value()
	if err != nil {
		return err
	}

	// TODO: Serialize row and write to the Row struct
	*row = *rowToInsert

	table.NumRows += 1

	return nil
}

func (t *Table) ExecuteSelect(stmt *Stmt) error {
	cursor := TableStart(t)
	for !cursor.endOfTable {
		row, err := cursor.Value()
		if err != nil {
			return err
		}
		printRow(row)
		cursor.Advance()
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
