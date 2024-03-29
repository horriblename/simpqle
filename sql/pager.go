package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/horriblename/simpqle/sql/bptree"
)

type Page [gPageSize]byte

type Pager struct {
	file     *os.File
	fileLen  uint64
	numPages uint64
	pages    [gTableMaxPages]*tmpTableNode
}

const tmpRootPageNum uint64 = 0

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
		file:     file,
		fileLen:  uint64(fileInfo.Size()),
		numPages: uint64(fileInfo.Size()) / gPageSize,
		pages:    [gTableMaxPages]*tmpTableNode{},
	}

	return pager, nil
}

func (pager *Pager) getPage(pageNum uint64) (*tmpTableNode, error) {
	if pageNum > gTableMaxPages {
		return nil, fmt.Errorf("Tried to fetch page number out of bounds. %d > %d", pageNum, gTableMaxPages)
	}

	if pager.pages[pageNum] == nil {
		if pageNum == tmpRootPageNum {
			rootNode := bptree.NewRootNode[uint64, Row]()
			pager.pages[pageNum] = &tmpTableNode{node: rootNode}
			return pager.pages[pageNum], nil
		}
		// Cache miss. allocate memory and load from file
		var page bptree.Node[uint64, Row]
		numPages := pager.fileLen / gPageSize

		// We might save a partial page at the end of the file
		if pager.fileLen%gPageSize != 0 {
			numPages += 1
		}

		if pageNum < numPages {
			var err error
			page, err = bptree.DeserializeBinary[uint64, Row](pager.file)
			if err != nil {
				return nil, err
			}
		}

		pager.pages[pageNum] = &tmpTableNode{node: page}

		if pageNum >= pager.numPages {
			pager.numPages = pageNum + 1
		}
	}

	return pager.pages[pageNum], nil
}

// flushes the selected page in memory into the database file
func (pager *Pager) flush(pageNum uint64) error {
	if pager.pages[pageNum] == nil {
		return errors.New("Tried to flush null page")
	}

	_, err := pager.file.Seek(int64(pageNum*gPageSize), io.SeekStart)
	if err != nil {
		return fmt.Errorf("Error seeking: %s", err)
	}

	err = serializePage((*tmpTableNode)(pager.pages[pageNum]), pager.file)
	if err != nil {
		return fmt.Errorf("Error writing: %s", err)
	}

	return nil
}

func serializePage(node *tmpTableNode, w io.Writer) error {
	return binary.Write(w, binary.BigEndian, node)
}

func deserializePage(r io.Reader) (tmpTableNode, error) {
	node := tmpTableNode{}
	err := binary.Read(r, binary.BigEndian, &node)
	return node, err
}
