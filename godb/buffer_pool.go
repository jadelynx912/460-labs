package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	maxPages int          // number of pages used is the length of the map
	pages    map[any]Page // to contain the data - this should be a map
	// TODO DONE: some code goes here
	logFile *LogFile
}

// Create a new BufferPool with the specified number of pages
// FIXME logFile nil for now, will be added later
func NewBufferPool(numPages int) (*BufferPool, error) {
	// TODO DONE: some code goes here
	var pagesMap = make(map[any]Page)
	return &BufferPool{maxPages: numPages, pages: pagesMap, logFile: nil}, nil // replace me
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	// TODO DONE: some code goes here
	for i := range bp.pages {
		bp.pages[i].getFile().flushPage(bp.pages[i])
		bp.pages[i].setDirty(-1, false) //FIXME using -1 as transaction id for now
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	// TODO DONE: some code goes here
	// For when the page is in the bufferPool
	key := file.pageKey(pageNo)
	page, ok := bp.pages[key]
	if ok {
		return page, nil
	}

	// For when the page is not in the bufferPool
	err := bp.evictPage()
	if err != nil {
		return nil, fmt.Errorf("Error evicting page, all pages dirty")
	}
	// add page to bp
	key = file.pageKey(pageNo)
	newPage, err := file.readPage(pageNo)
	if err != nil {
		return nil, fmt.Errorf("Error reading page from file")
	}
	bp.pages[key] = newPage
	return newPage, nil
}

// Hint: GetPage function need function there: TODO DONE
func (bp *BufferPool) evictPage() error {
	if len(bp.pages) < bp.maxPages { // Don't need to evict a page
		return nil
	}
	for key, page := range bp.pages { //Maps unordered, evict first non dirty page. No moving items needed
		if !page.isDirty() {
			delete(bp.pages, key)
			return nil
		}
	}
	return fmt.Errorf("evictPage() all pages are dirty")
}
