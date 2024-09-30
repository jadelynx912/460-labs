package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	desc   *TupleDesc
	pageID any //type heapHash but since that function returns any
	pageNo int
	dirty  bool
	file   *HeapFile
	tuples map[recordID]Tuple
	// TODO DONE: some code goes here
	sync.Mutex
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// TODO DONE: some code goes here
	return &heapPage{desc: desc,
		pageID: f.pageKey(pageNo),
		pageNo: pageNo,
		dirty:  false,
		file:   f,
		tuples: make(map[recordID]Tuple)}, nil
}

// Hint TODO DONE: heapfile/insertTuple needs function there:
func (h *heapPage) getNumEmptySlots() int {
	return h.getNumSlots() - len(h.tuples)
}

func (h *heapPage) getNumSlots() int {
	// TODO DONE: some code goes here
	var remPageSize = PageSize - 8 // bytes after header
	var numSlots = remPageSize / h.desc.bytesPerTuple()

	return numSlots //replace me
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
// FIXME may need to redo inheritance with recordID and heapFileRid
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO DONE: some code goes here
	slots := h.getNumEmptySlots()
	if slots == 0 {
		return nil, fmt.Errorf("insertTuple() no empty slots in the page")
	}
	//FIXME need to redo this - need there to be no way that id of tuples < len(tuples), no gaps in ids
	for i := 0; i < h.getNumSlots(); i++ {
		newRid := heapFileRid{pageNum: h.pageNo, slotNum: i}
		t.Rid = newRid
		_, ok := h.tuples[newRid]
		if !ok {
			h.tuples[newRid] = *t
			return newRid, nil
		}
	}

	return nil, fmt.Errorf("made it through the whole map with empty slots (?) without finding one")
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO DONE: some code goes here
	_, ok := h.tuples[rid]
	if ok {
		delete(h.tuples, rid)
		return nil
	}
	return fmt.Errorf("deleteTuple - recordId doesn't exist")
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO DONE: some code goes here
	return h.dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	// TODO DONE: some code goes here
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	// TODO DONE: some code goes here
	return p.file //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO DONE: some code goes here
	var buf = bytes.NewBuffer(make([]byte, 0, PageSize))
	// Write headers
	err := binary.Write(buf, binary.LittleEndian, int32(h.getNumSlots()))
	if err != nil {
		return nil, fmt.Errorf("error writing to buffer")
	}
	err = binary.Write(buf, binary.LittleEndian, int32(h.getNumEmptySlots()))
	if err != nil {
		return nil, fmt.Errorf("error writing to buffer")
	}

	for _, tuple := range h.tuples {
		err = tuple.writeTo(buf)
		if err != nil {
			return nil, fmt.Errorf("error writing to buffer")
		}
	}

	if buf.Len() < PageSize {
		buf.Write(bytes.Repeat([]byte("0"), PageSize-buf.Len()))
	}

	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO DONE: some code goes here
	// Read headers
	numSlots := make([]byte, 4)
	numEmptySlots := make([]byte, 4)
	err := binary.Read(buf, binary.LittleEndian, &numSlots)
	if err != nil {
		return fmt.Errorf("error reading from buffer")
	}
	err = binary.Read(buf, binary.LittleEndian, &numEmptySlots)
	if err != nil {
		return fmt.Errorf("error reading from buffer")
	}

	slots := int(binary.LittleEndian.Uint32(numSlots))
	emptySlots := int(binary.LittleEndian.Uint32(numEmptySlots))

	for i := 0; i < slots-emptySlots; i++ {
		newTuple, err := readTupleFrom(buf, h.desc)
		if err != nil {
			return fmt.Errorf("initFromBuffer error reading tuple")
		}
		key := heapFileRid{pageNum: h.pageNo, slotNum: i}
		newTuple.Rid = key
		h.tuples[key] = *newTuple
	}
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO DONE: some code goes here
	// Make a slice of keys so the order of the map is preserved - iterating over that
	keys := make([]heapFileRid, 0, len(p.tuples))
	for k := range p.tuples {
		rid, ok := k.(heapFileRid)
		if ok {
			keys = append(keys, rid)
		} else {
			return func() (*Tuple, error) {
				return nil, fmt.Errorf("error with record id types")
			}
		}
	}
	i := 0

	return func() (*Tuple, error) {
		if i >= len(keys) {
			return nil, nil
		}
		val := p.tuples[keys[i]]
		i++
		return &val, nil
	}
}
