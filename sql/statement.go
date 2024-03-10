package sql

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"
)

type StmtKind int

type Stmt struct {
	Kind        StmtKind
	RowToInsert *Row
}

type Row struct {
	Id       int64
	Username colUsername
	Email    colEmail
}

type colUsername [gColumnUsernameSize]byte
type colEmail [gColumnEmailSize]byte

type Page [gPageSize]byte

type Table struct {
	NumRows uint64
	pager   *Pager
}

type Pager struct {
	file    *os.File
	fileLen uint64
	pages   [gTableMaxPages]unsafe.Pointer
}

const (
	Insert = iota
	Select
)

const (
	gColumnUsernameSize uint64 = 32
	gColumnEmailSize    uint64 = 255
	gRowSize            uint64 = 8 /* sizeOf(uint64) */ + gColumnUsernameSize + gColumnEmailSize

	// virtual memory system of most computer architectures use page size of 4kB?
	gPageSize      uint64 = 4096
	gTableMaxPages uint64 = 100

	gRowsPerPage  uint64 = gPageSize / gRowSize
	gTableMaxRows uint64 = gRowsPerPage * gTableMaxPages
)

var (
	// PrepareStmt errors
	ErrSyntax                = errors.New("syntax error")
	ErrInsertOversizedColumn = errors.New("inserted column too large")

	ErrUnknownStmt = errors.New("unknown statement")
	ErrTableFull   = errors.New("table is full")
)

func PrepareStmt(input string) (stmt Stmt, err error) {
	if strings.HasPrefix(input, "insert") {
		var usernameStr, emailStr string
		var username colUsername
		var email colEmail
		var id int64
		parsed, err := fmt.Sscanf(input, "insert %d %s %s", &id, &usernameStr, &emailStr)

		if len(username) < len(usernameStr) {
			return stmt, ErrInsertOversizedColumn
		}
		for i, c := range []byte(usernameStr) {
			username[i] = c
		}

		if len(email) < len(emailStr) {
			return stmt, ErrInsertOversizedColumn
		}
		for i, c := range []byte(emailStr) {
			email[i] = c
		}

		if err != nil {
			return stmt, fmt.Errorf("preparing statement: %w", err)
		}
		if parsed != 3 {
			return stmt, ErrSyntax
		}

		return Stmt{
			Kind:        Insert,
			RowToInsert: &Row{id, username, email},
		}, nil
	} else if strings.HasPrefix(input, "select") {
		return Stmt{Kind: Select}, nil
	}

	return Stmt{}, ErrUnknownStmt
}

func (row *Row) Serialize() (b []byte, err error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(row)
	return buf.Bytes(), err
}

func Deserialize(b []byte) (Row, error) {
	row := Row{}
	dec := gob.NewDecoder(bytes.NewReader(b))
	err := dec.Decode(&row)
	return row, err
}

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

func pagerOpen(fname string) (*Pager, error) {
	const userReadPerm = 0o400
	const userWritePerm = 0o200
	file, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, userReadPerm|userWritePerm)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	pager := &Pager{
		file:    file,
		fileLen: uint64(fileInfo.Size()),
		pages:   [gTableMaxPages]unsafe.Pointer{},
	}

	return pager, nil
}

func (pager *Pager) getPage(pageNum uint64) (unsafe.Pointer, error) {
	if pageNum > gTableMaxPages {
		return nil, fmt.Errorf("Tried to fetch page number out of bounds. %d > %d", pageNum, gTableMaxPages)
	}

	if pager.pages[pageNum] == nil {
		// Cache miss. allocate memory and load from file
		page := &Page{}
		numPages := pager.fileLen / gPageSize

		// We might save a partial page at the end of the file
		if pager.fileLen%gPageSize != 0 {
			numPages += 1
		}

		if pageNum < numPages {
			_, err := pager.file.ReadAt(page[:], int64(pageNum*gPageSize))
			if err != nil {
				return nil, fmt.Errorf("Error reading file: %w", err)
			}
		}

		pager.pages[pageNum] = unsafe.Pointer(page)
	}

	return unsafe.Pointer(pager.pages[pageNum]), nil
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

// flushes the selected page in memory into the database file
func (pager *Pager) flush(pageNum uint64, size uint64) error {
	if pager.pages[pageNum] == nil {
		return errors.New("Tried to flush null page")
	}

	_, err := pager.file.Seek(int64(pageNum*gPageSize), io.SeekStart)
	if err != nil {
		return fmt.Errorf("Error seeking: %s", err)
	}

	bytesToWrite := (*[gRowsPerPage * gPageSize]byte)(pager.pages[pageNum])[:size]
	_, err = pager.file.Write(bytesToWrite)
	if err != nil {
		return fmt.Errorf("Error writing: %s", err)
	}

	return nil
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}
