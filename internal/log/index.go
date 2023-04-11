package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4                   // The number of bytes used for position in the index
	posWidth uint64 = 8                   // The number of bytes used for position in store
	entWidth        = offWidth + posWidth // The total number of bytes used for a index offset -> store position mapping
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// Get statistics on file
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Set index size to size of file because we always trim the empty space
	// when closing the index. So this is safe
	idx.size = uint64(fi.Size())

	// Since we trim the file when safely closing the index, we need to increase the size
	// to the max segment size when recreating the index before the mmap call
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// Now that the file is the full size of a segment, we can make the mmap call to reserve
	// the virtual address space for the app
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {

	// Flushes data in mmap virtual address space
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// Flushes written file data that is in os cache/buffer and not on disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Trims file to the size of the index instead of max segment length. This is critical
	// when starting the app up again
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {

	// If nothing is in the index, return EOF error
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// Special case to return the last mmap array "index"
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)

		// The 'in' variable is 'out' variable mmap array index
		// when its converted to a 4 byte value
	} else {
		out = uint32(in)
	}

	// Out is the offset of the index of the mmap array starting at the index's value(a 4 byte number)
	// Multiplying this by the 'entWidth' gives you the exact starting spot in the mmap for the index offset and store position
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// The location in the index is 'pos' to 4 bytes more
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	// The location in the store is ()'pos' plus 4 bytes) to (pos plus 12 bytes)
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {

	// This means the index is full
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Append a 4 byte 'offset' (which is more or less an
	// incrementing primary key for each 'entWidth' chhunk)
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// Append a 8 byte 'pos' value which is the place in
	// the store the value is located
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// The size of index always increases by 12 bytes
	i.size += uint64(entWidth)

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
