package sql

import (
	"errors"
	"strings"
)

type StmtKind int

type Stmt struct {
	kind StmtKind
}

const (
	Insert = iota
	Select
)

var ErrUnknownStmt = errors.New("unknown statement")

func PrepareStmt(input string) (Stmt, error) {
	if strings.HasPrefix(input, "insert") {
		return Stmt{Insert}, nil
	} else if strings.HasPrefix(input, "select") {
		return Stmt{Select}, nil
	}

	return Stmt{}, ErrUnknownStmt
}
