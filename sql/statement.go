package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
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
	pages   [gTableMaxPages]*Page
}

const (
	Insert = iota
	Select
)

const (
	gColumnUsernameSize uint64 = 32
	gColumnEmailSize    uint64 = 255
	gRowSize            uint64 = 8 /* int64 */ + gColumnUsernameSize + gColumnEmailSize

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
	buf := bytes.NewBuffer([]byte{})
	err = binary.Write(buf, binary.BigEndian, row.Id)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, colUsername(row.Username))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, byte(0))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, colEmail(row.Email))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, byte(0))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (row *Row) GobSer() (b []byte, err error) {
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

func rowSlot(table *Table, rowNum uint64) unsafe.Pointer {
	pageNum := rowNum / gRowsPerPage
	page := table.pages[pageNum]
	if page == nil {
		// Allocate memory only when we try to access page
		table.pages[pageNum] = &Page{}
		page = table.pages[pageNum]
	}

	rowOffset := rowNum % gRowsPerPage
	byteOffset := rowOffset % gRowSize
	return unsafe.Pointer(uintptr(unsafe.Pointer(page)) + uintptr(byteOffset))
}

func (table *Table) ExecuteInsert(stmt *Stmt) error {
	if table.NumRows >= gTableMaxRows {
		return ErrTableFull
	}

	rowToInsert := stmt.RowToInsert

	rowPtr := (*Row)(rowSlot(table, table.NumRows))
	*rowPtr = *rowToInsert

	table.NumRows += 1

	return nil
}

func (t *Table) ExecuteSelect(stmt *Stmt) error {
	for i := uint64(0); i < t.NumRows; i++ {
		row := (*Row)(rowSlot(t, i))
		printRow(row)
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

func newTable() *Table {
	return &Table{}
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}
