package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
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

func (row *Row) Serialize(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, row)
}

func Deserialize(r io.Reader) (Row, error) {
	row := Row{}
	err := binary.Read(r, binary.BigEndian, &row)
	return row, err
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}
