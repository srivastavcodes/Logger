package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + lenWidth)
)

func TestStore_Append_Read(t *testing.T) {
	file, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)

	defer os.Remove(file.Name())
	defer file.Close()

	st, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, st)
	testRead(t, st)
	testReadAt(t, st)

	st, err = newStore(file)
	require.NoError(t, err)

	testRead(t, st)
}

func testAppend(t *testing.T, st *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		wid, pos, err := st.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+wid, wid*i)
	}
}

func testRead(t *testing.T, st *store) {
	t.Helper()

	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := st.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, st *store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		data := make([]byte, lenWidth)
		wid, err := st.ReadAt(data, off)

		require.NoError(t, err)
		require.Equal(t, wid, lenWidth)
		off += int64(wid)

		size := enc.Uint64(data)
		data = make([]byte, size)

		wid, err = st.ReadAt(data, off)
		require.NoError(t, err)
		require.Equal(t, write, data)
		require.Equal(t, int(size), wid)

		off += int64(wid)
	}
}

func TestStoreClose(t *testing.T) {
	file, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	defer file.Close()

	s, err := newStore(file)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	file, beforeSize, err := openFile(file.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(file.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
