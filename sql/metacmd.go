package sql

import (
	"errors"
	"fmt"
	"os"
)

var (
	ExitCmd               = errors.New(".exit called")
	ErrUnknownMetaCommand = errors.New("unknwon meta command")
)

func DoMetaCommand(input string, table *Table) error {
	switch input {
	case ".exit":
		return ExitCmd
	case ".btree":
		rootNode, err := table.pager.getPage(0)
		if err != nil {
			return err
		}

		fmt.Fprintln(os.Stderr, rootNode.node.Visualize())
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrUnknownMetaCommand, input)
	}
}
