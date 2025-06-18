package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File

	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(file *os.File) (*store, error) {
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: file,
		size: size,
		buf:  bufio.NewWriter(file),
	}, nil
}

func (st *store) Append(data []byte) (wid uint64, pos uint64, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	pos = st.size

	err = binary.Write(st.buf, enc, uint64(len(data)))
	if err != nil {
		return 0, 0, err
	}
	width, err := st.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}
	width += lenWidth

	st.size += uint64(width)
	return uint64(width), pos, nil
}

func (st *store) Read(pos uint64) ([]byte, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)

	_, err := st.File.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}
	data := make([]byte, enc.Uint64(size))

	_, err = st.File.ReadAt(data, int64(pos+lenWidth))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (st *store) ReadAt(data []byte, off int64) (int, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.buf.Flush(); err != nil {
		return 0, err
	}
	return st.File.ReadAt(data, off)
}

func (st *store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	err := st.buf.Flush()
	if err != nil {
		return err
	}
	return st.File.Close()
}
