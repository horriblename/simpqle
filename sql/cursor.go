package sql

import "unsafe"

type Cursor struct {
	table      *Table
	rowNum     uint64
	endOfTable bool
}

func TableStart(table *Table) *Cursor {
	return &Cursor{
		table:      table,
		rowNum:     0,
		endOfTable: table.NumRows == 0,
	}

}

func TableEnd(table *Table) *Cursor {
	return &Cursor{
		table:      table,
		rowNum:     table.NumRows,
		endOfTable: true,
	}
}

func (cursor *Cursor) Advance() {
	cursor.rowNum += 1
	if cursor.rowNum >= cursor.table.NumRows {
		cursor.endOfTable = true
	}
}

func (cursor *Cursor) Value() (*Row, error) {
	rowNum := cursor.rowNum
	pageNum := rowNum / gRowsPerPage
	page, err := cursor.table.pager.getPage(pageNum)
	if err != nil {
		return nil, err
	}

	rowOffset := rowNum % gRowsPerPage
	byteOffset := rowOffset * gRowSize
	return (*Row)(unsafe.Pointer(uintptr(unsafe.Pointer(page)) + uintptr(byteOffset))), nil
}
