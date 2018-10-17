// package gig implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
package gig

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
)

// checkAndCreate may create dirs
func checkAndCreate(path string) (bool, error) {
	// detect if file exists
	var _, err = os.Stat(path)
	if err == nil {
		return true, err
	}
	// create dirs if file not exists
	if os.IsNotExist(err) {
		if dir := filepath.Dir(path); dir != "." {
			return false, os.MkdirAll(dir, DIR_MODE)
		}
	}
	return false, err
}

type readResponse struct {
	val []byte
	err error
}

type readRequest struct {
	readKey      string
	responseChan chan readResponse
}

type writeResponse struct {
	err error
}

type writeRequest struct {
	readKey      string
	writeVal     []byte
	responseChan chan writeResponse
}

type deleteRequest struct {
	deleteKey    string
	responseChan chan struct{}
}

type keysResponse struct {
	keys [][]byte
}

type keysRequest struct {
	fromKey      []byte
	limit        uint32
	offset       uint32
	asc          bool
	responseChan chan keysResponse
}

type setsResponse struct {
	err error
}

type setsRequest struct {
	pairs        [][]byte
	responseChan chan setsResponse
}

type getsResponse struct {
	pairs [][]byte
}

type getsRequest struct {
	keys         [][]byte
	responseChan chan getsResponse
}

type hasResponse struct {
	exists bool
}

type hasRequest struct {
	key          string
	responseChan chan hasResponse
}

type counterGetResponse struct {
	counter uint64
}

type counterGetRequest struct {
	key          string
	responseChan chan counterGetResponse
}

type counterSetRequest struct {
	key          string
	counter      uint64
	store        bool
	responseChan chan struct{}
}

// DB store channels with requests
type DB struct {
	readRequests       chan readRequest
	writeRequests      chan writeRequest
	deleteRequests     chan deleteRequest
	keysRequests       chan keysRequest
	setsRequests       chan setsRequest
	getsRequests       chan getsRequest
	hasRequests        chan hasRequest
	counterGetRequests chan counterGetRequest
	counterSetRequests chan counterSetRequest
}

// internal set
func (db *DB) setKey(key string, val []byte) error {
	c := make(chan writeResponse)
	w := writeRequest{readKey: key, writeVal: val, responseChan: c}
	db.writeRequests <- w
	resp := <-c
	return resp.err
}

// internal get
func (db *DB) readKey(key string) ([]byte, error) {
	c := make(chan readResponse)
	w := readRequest{readKey: key, responseChan: c}
	db.readRequests <- w
	resp := <-c
	return resp.val, resp.err
}

// internal delete
func (db *DB) deleteKey(key string) {
	c := make(chan struct{})
	d := deleteRequest{deleteKey: key, responseChan: c}
	db.deleteRequests <- d
	<-c
}

// internal keys
func (db *DB) readKeys(from []byte, limit, offset uint32, asc bool) [][]byte {
	c := make(chan keysResponse)
	w := keysRequest{responseChan: c, fromKey: from, limit: limit, offset: offset, asc: asc}
	db.keysRequests <- w
	resp := <-c
	return resp.keys
}

// internal sets
func (db *DB) sets(setPairs [][]byte) error {
	c := make(chan setsResponse)
	w := setsRequest{pairs: setPairs, responseChan: c}
	db.setsRequests <- w
	resp := <-c
	return resp.err
}

// internal gets
func (db *DB) gets(keys [][]byte) [][]byte {
	c := make(chan getsResponse)
	w := getsRequest{keys: keys, responseChan: c}
	db.getsRequests <- w
	resp := <-c
	return resp.pairs
}

// internal has
func (db *DB) has(key string) bool {
	c := make(chan hasResponse)
	w := hasRequest{key: key, responseChan: c}
	db.hasRequests <- w
	resp := <-c
	return resp.exists
}

// internal counter
func (db *DB) counterGet(key string) uint64 {
	c := make(chan counterGetResponse)
	w := counterGetRequest{key: key, responseChan: c}
	db.counterGetRequests <- w
	resp := <-c
	return resp.counter
}

// internal counter
func (db *DB) counterSet(key string, counterNewVal uint64, store bool) {
	c := make(chan struct{})
	w := counterSetRequest{key: key, counter: counterNewVal, store: store, responseChan: c}
	db.counterSetRequests <- w
	<-c
}

// internal counter
func (db *DB) countKeys() uint64 {
	return db.counterGet(NAME_COUNT_KEYS)
}

// newDB Create new DB
// it return error if any
// DB has finalizer for canceling goroutine
// File will be created (with dirs) if not exist
func newDB(file string) (*DB, error) {
	ctx, cancel := context.WithCancel(context.Background())
	readRequests := make(chan readRequest)
	writeRequests := make(chan writeRequest)
	deleteRequests := make(chan deleteRequest)
	keysRequests := make(chan keysRequest)
	setsRequests := make(chan setsRequest)
	getsRequests := make(chan getsRequest)
	hasRequests := make(chan hasRequest)
	counterGetRequests := make(chan counterGetRequest)
	counterSetRequests := make(chan counterSetRequest)
	d := &DB{
		readRequests:       readRequests,
		writeRequests:      writeRequests,
		deleteRequests:     deleteRequests,
		keysRequests:       keysRequests,
		setsRequests:       setsRequests,
		getsRequests:       getsRequests,
		hasRequests:        hasRequests,
		counterGetRequests: counterGetRequests,
		counterSetRequests: counterSetRequests,
	}
	// This is a lambda, so we don't have to add members to the struct
	runtime.SetFinalizer(d, func(db *DB) {
		cancel()
	})

	exists, err := checkAndCreate(file)
	if exists && err != nil {
		cancel()
		return nil, err
	}
	opts := os.O_CREATE | os.O_RDWR
	//files
	fk, err := os.OpenFile(file+KEY_FILE_EXT, opts, FILE_MODE)
	//fk, err := syncfile.NewSyncFile(file+KEY_FILE_EXT, FILE_MODE)
	if err != nil {
		cancel()
		return nil, err
	}
	fv, err := os.OpenFile(file+VAL_FILE_EXT, opts, FILE_MODE)
	if err != nil {
		cancel()
		return nil, err
	}

	// We can't have run be a method of DB, because otherwise then the goroutine will keep the reference alive
	go run(ctx, fk, fv, readRequests, writeRequests, deleteRequests, keysRequests, setsRequests, getsRequests,
		hasRequests, counterGetRequests, counterSetRequests)

	return d, nil
}
