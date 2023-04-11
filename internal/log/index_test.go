package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// Since there is no data in the index, it return io.EOF error
	_, _, err = idx.Read(-1)
	require.Error(t, err)

	// index extends file so they have the same name
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		// Adds 4 bytes for index offset and 8 bytes for store position
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		// Uses 4 byte index offset as a 8 byte value as input,
		// should return 'want.Pos' value
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// index and scanner shoud error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	// Gets the last "offset" in index and last "pos" in store
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)

	// Since we have 2 entries, '1' is the last "offset" value
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
