package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Size equals the total number of 0s and 1s in this store
	// with uint64 giving it a max size of 18446744073709551615
	// which is 18446744073 Gigabytes, 18446744 Terabytes, or 18446 Petabytes
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// Write the size of this piece of data into the store as an 8 byte value
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the data into the buffer. This is directly after the
	// uint64 saying how big this chunk is
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	// current size +
	// 8 bytes(a uint64 for the number of bytes for this data stored in) +
	// data in bytes
	// = new size
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//flush buffer so all data is present when reading
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Equivalent to a uint64 sized byte array
	size := make([]byte, lenWidth)

	// Read in a uint64 that represents how big the data is starting
	// from the byte represented by 'pos'
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Create a byte array the size of the next piece of data
	b := make([]byte, enc.Uint64(size))

	// b is the exact size of the data, read in all the data to b
	// starting at pos + 8 bytes
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush buffer in case we are reading data that is currently
	// in it
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	// Get the data starting at an offset until you fill up the byte array p
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
