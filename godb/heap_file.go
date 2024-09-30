package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	backingFile string
	desc        TupleDesc
	numPages    int
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	sync.Mutex
}

// Hint TODO DONE: heap_page and heap_file need function there:
type heapFileRid struct {
	pageNum int
	slotNum int
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	//Calculate the initial number of pages from the file
	file, err := os.Open(fromFile)
	if err != nil {
		return &HeapFile{
			backingFile: fromFile,
			desc:        *td,
			numPages:    0,
			bufPool:     bp,
		}, nil
	}
	defer file.Close()

	size, err := file.Stat()
	if err != nil {
		return &HeapFile{
			backingFile: fromFile,
			desc:        *td,
			numPages:    0,
			bufPool:     bp,
		}, fmt.Errorf("error getting file size after it was opened")
	}
	num := int(size.Size()) / PageSize

	return &HeapFile{
		backingFile: fromFile,
		desc:        *td,
		numPages:    num,
		bufPool:     bp,
	}, nil //replace me
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	// TODO DONE: some code goes here
	return f.backingFile //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO DONE: some code goes here
	return f.numPages
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	// TODO DONE: some code goes here
	file, err := os.Open(f.backingFile)
	if err != nil {
		return nil, fmt.Errorf("readPage - error opening the file")
	}
	defer file.Close()

	_, err = file.Seek(int64(pageNo)*int64(PageSize), 0) // 0 - relative to start of file
	if err != nil {
		return nil, fmt.Errorf("readPage - error moving through the file")
	}

	var buffer bytes.Buffer
	reader := io.LimitReader(file, int64(PageSize))
	_, err = io.Copy(&buffer, reader)
	if err != nil {
		return nil, fmt.Errorf("readPage - error opening the file")
	}

	p, err := newHeapPage(&f.desc, pageNo, f)
	if err != nil {
		return nil, fmt.Errorf("error creating page")
	}

	err = p.initFromBuffer(&buffer)
	if err != nil {
		return nil, fmt.Errorf("error initializing page")
	}

	return p, nil
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
// FIXME write it now? or later when it gets flushed
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO DONE: some code goes here
	numPages := f.NumPages()
	for i := 0; i < numPages; i++ {
		page, err := f.bufPool.GetPage(f, i, tid, -1)
		if err != nil {
			return fmt.Errorf("insertTuple() error getting the page from the file")
		}
		// Typecast
		p, ok := page.(*heapPage)
		if !ok {
			return fmt.Errorf("insertTuple() error with the page type")
		}

		if p.getNumEmptySlots() > 0 {
			_, err := p.insertTuple(t)
			if err != nil {
				return fmt.Errorf("insertTuple() error inserting the tuple")
			}
			p.setDirty(tid, true)
			return nil
		}
	}

	// If no empty pages
	newPage, err := newHeapPage(&f.desc, numPages, f) //Want numPages to be pageNo since we're indexing now
	if err != nil {
		return fmt.Errorf("insertTuple() error creating a new page")
	}
	f.numPages += 1
	// Need to add the newly created page to the buffer pool or it disappears
	err = f.bufPool.evictPage()
	if err != nil {
		return fmt.Errorf("insertTuple() error inserting page in bufferpool")
	}

	key := f.pageKey(newPage.pageNo)
	_, ok := f.bufPool.pages[key]
	if ok {
		return fmt.Errorf("the pageId already exists")
	}
	f.bufPool.pages[key] = newPage

	_, err = newPage.insertTuple(t)
	if err != nil {
		return fmt.Errorf("insertTuple() error inserting tuple in new page")
	}
	newPage.setDirty(tid, true)
	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO DONE: some code goes here
	id, ok := t.Rid.(heapFileRid)
	if !ok {
		return fmt.Errorf("deleteTuple() error typecasting id")
	}

	page, err := f.bufPool.GetPage(f, id.pageNum, tid, -1)
	if err != nil {
		return fmt.Errorf("deleteTuple error reading page")
	}
	p, ok := page.(*heapPage)
	if !ok {
		return fmt.Errorf("pageType wrong")
	}

	err = p.deleteTuple(id)
	if err != nil {
		return fmt.Errorf("error deleting tuple")
	}

	p.setDirty(tid, true)
	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	// TODO DONE: some code goes here
	page, ok := p.(*heapPage)
	if !ok {
		return fmt.Errorf("page not a heapPage")
	}

	buf, err := page.toBuffer()
	if err != nil {
		return fmt.Errorf("flushPage - error opening the file")
	}

	file, err := os.OpenFile(f.backingFile, os.O_CREATE|os.O_WRONLY, 0666) // Everyone can write to file - hopefully
	if err != nil {
		return fmt.Errorf("flushPage - error opening the file")
	}
	defer file.Close()

	_, err = file.WriteAt(buf.Bytes(), int64(page.pageNo)*int64(PageSize))
	if err != nil {
		return fmt.Errorf("flushPage - error writing to the file")
	}

	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO DONE: some code goes here
	return &f.desc //replace me

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	numPages := f.NumPages()
	i := 0
	var val *Tuple = nil
	var err error = nil

	// For empty heapfile
	if numPages == 0 {
		return func() (*Tuple, error) {
			return nil, nil
		}, nil
	}

	page, err := f.bufPool.GetPage(f, i, tid, -1)
	if err != nil {
		return nil, fmt.Errorf("error getting a page")
	}
	p, ok := page.(*heapPage)
	if !ok {
		return nil, fmt.Errorf("error casting page to heapPage")
	}

	tupleIterFunc := p.tupleIter()

	return func() (*Tuple, error) { //replace me
		if i == numPages { //handles empty heapfile though that might throw an error
			return nil, nil
		}

		val, err = tupleIterFunc()

		if val == nil && err == nil {
			i += 1
			if i == numPages {
				return nil, nil
			}

			page, err := f.bufPool.GetPage(f, i, tid, -1)
			if err != nil {
				return nil, fmt.Errorf("error getting a page")
			}
			p, ok := page.(*heapPage)
			if !ok {
				return nil, fmt.Errorf("error casting page to heapPage")
			}

			tupleIterFunc = p.tupleIter()
			val, err = tupleIterFunc()
		}
		return val, err
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	// TODO DONE: some code goes here
	return heapHash{
		FileName: f.backingFile,
		PageNo:   pgNo,
	}
}
