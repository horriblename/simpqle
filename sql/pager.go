package sql

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

type Page [gPageSize]byte

type Pager struct {
	file    *os.File
	fileLen uint64
	pages   [gTableMaxPages]unsafe.Pointer
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
