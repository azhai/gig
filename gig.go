// package gig implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
package gig

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"runtime"
	"sync"
)

const (
	// FILE_MODE - file will be created in this mode
	FILE_MODE       = 0666
	DIR_MODE        = 0777
	KEY_FILE_EXT    = ".gik"
	VAL_FILE_EXT    = ".giv"
	NAME_COUNT_KEYS = "_LEN_KEYS_"
)

var (
	stores = make(map[string]*DB)
	mutex  = &sync.RWMutex{}

	// ErrKeyNotFound - key not found
	ErrKeyNotFound = errors.New("Error: key not found")
	// ErrDbOpened - db is opened
	ErrDbOpened = errors.New("Error: db is opened")
	// ErrDbNotOpen - db not open
	ErrDbNotOpen = errors.New("Error: db not open")

	bufPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

type Row interface {
	GetId() uint32
	SetId(id uint32) error
}

type Collection interface {
	Clear() error
	Close() error
	Reconnect() error
	Count() int
	Save(rows ...Row) error
	All(limit int, asc bool) ([]Row, error)
}

// Open open/create DB (with dirs)
// This operation is locked by mutex
// Return error if any
// Create .idx file for key storage
func Open(file string) (db *DB, err error) {
	mutex.Lock()
	defer mutex.Unlock()

	v, ok := stores[file]
	if ok {
		return v, nil
	}

	//fmt.Println("NewDB")
	db, err = newDB(file)
	if err == nil {
		stores[file] = db
	}
	return db, err
}

// Close - close DB and free used memory
// It run finalizer and cancel goroutine
func Close(file string) (err error) {
	mutex.Lock()
	defer mutex.Unlock()
	//db, ok := stores[file]
	_, ok := stores[file]
	if !ok {
		return ErrDbNotOpen
	}
	// store counters if present
	//db.counterSet("", 0, true)
	delete(stores, file)
	/* Force GC, to require finalizer to run */
	runtime.GC()
	return err
}

// CloseAll - close all opened DB
func CloseAll() (err error) {

	for k := range stores {
		err = Close(k)
		if err != nil {
			break
		}
	}

	return err
}

// DeleteFile close file key and file val and delete db from map and disk
// All data will be loss!
func DeleteFile(file string) (err error) {
	Close(file)

	err = os.Remove(file + KEY_FILE_EXT)
	if err != nil {
		return err
	}
	err = os.Remove(file + VAL_FILE_EXT)
	return err
}

// Set store val and key with sync at end
// File - may be existing file or new
// If path to file contains dirs - dirs will be created
// If val is nil - will store only key
func Set(file string, key []byte, val []byte) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	err = db.setKey(string(key), val)
	return err
}

// Put store val and key with sync at end. It's wrapper for Set.
func Put(file string, key []byte, val []byte) (err error) {
	return Set(file, key, val)
}

// SetGob - experimental future for lazy usage, see tests
func SetGob(file string, key interface{}, val interface{}) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	bufKey := bytes.Buffer{}
	bufVal := bytes.Buffer{}

	if reflect.TypeOf(key).String() == "[]uint8" {
		v := key.([]byte)
		_, err = bufKey.Write(v)
	} else {
		err = gob.NewEncoder(&bufKey).Encode(key)
	}

	if err != nil {
		return err
	}
	err = gob.NewEncoder(&bufVal).Encode(val)
	if err != nil {
		return err
	}

	err = db.setKey(bufKey.String(), bufVal.Bytes())
	//fmt.Println(bufKey.Bytes())
	return err
}

// Has return true if key exist or error if any
func Has(file string, key []byte) (exist bool, err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return false, err
	}
	exist = db.has(string(key))
	return exist, err
}

// Count return count of keys or error if any
func Count(file string) (cnt uint64, err error) {
	db, err := Open(file)
	if err != nil {
		return 0, err
	}
	cnt = db.countKeys()
	return cnt, err
}

// Counter return unique uint64
func Counter(file string, key []byte) (counter uint64, err error) {
	db, err := Open(file)
	if err != nil {
		return 0, err
	}

	val, err := db.readKey(string(key))
	if err == nil {
		if val == nil || len(val) != 8 {
			return counter, ErrKeyNotFound
		}
		counter = binary.BigEndian.Uint64(val)
		counter++
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, counter)
		err = db.setKey(string(key), b)
		return counter, err
	}
	if err == ErrKeyNotFound {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(1))
		err = db.setKey(string(key), b)
		return uint64(1), err
	}
	return counter, err

	/*
			counter = db.counterGet(string(key))
			//fmt.Println("Counter", counter)
			if counter == 1 {
				// new counter
				b, _ := db.readKey(string(key))
				if b != nil && len(b) == 8 {
					//recover in case of panic?
					counter = binary.BigEndian.Uint64(b)
					counter++
				}

				db.counterSet(string(key), counter, false)
			}

		return counter, err
	*/
}

// Get return value by key or nil and error
// Get will open DB if it closed
// return error if any
func Get(file string, key []byte) (val []byte, err error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val, err = db.readKey(string(key))
	return val, err
}

// GetGob - experimental future for lazy usage, see tests
func GetGob(file string, key interface{}, val interface{}) (err error) {
	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	//fmt.Println(reflect.TypeOf(key))

	if reflect.TypeOf(key).String() == "[]uint8" {
		v := key.([]byte)
		_, err = buf.Write(v)
	} else {
		err = gob.NewEncoder(buf).Encode(key)
	}

	if err != nil {
		return err
	}

	bin, err := db.readKey(buf.String())
	buf.Reset()
	if err != nil {
		return err
	}
	buf.Write(bin)
	err = gob.NewDecoder(buf).Decode(val)

	return err

}

// Keys return keys in ascending  or descending order (false - descending,true - ascending)
// if limit == 0 return all keys
// if offset>0 - skip offset records
// If from not nil - return keys after from (from not included)
// If last byte of from == "*" - return keys with this prefix
func Keys(file string, from []byte, limit, offset uint32, asc bool) ([][]byte, error) {
	db, err := Open(file)
	if err != nil {
		return nil, err
	}
	val := db.readKeys(from, limit, offset, asc)
	return val, err
}

// Gets return key/value pairs in random order
// result contains key and value
// Gets not return error if key not found
// If no keys found return empty result
func Gets(file string, keys [][]byte) (result [][]byte) {
	db, err := Open(file)
	if err != nil {
		return nil
	}
	return db.gets(keys)
}

// Sets store vals and keys
// Sync will called only at end of insertion
// Use it for mass insertion
// every pair must contain key and value
func Sets(file string, pairs [][]byte) (err error) {

	db, err := Open(file)
	//fmt.Println("set", db, err)
	if err != nil {
		return err
	}
	err = db.sets(pairs)
	return err
}

// Delete key (always return true)
// Delete not remove any data from files
// Return error if any
func Delete(file string, key []byte) (deleted bool, err error) {
	db, err := Open(file)
	if err != nil {
		return deleted, err
	}
	db.deleteKey(string(key))
	return true, err
}

func Id2Bin(id uint32) []byte {
	bin := make([]byte, 4)
	binary.BigEndian.PutUint32(bin, id)
	return bin
}

type Gig struct {
	file       string
	db         *DB
	RowCreator func() Row
}

func NewGig(path string, creator func() Row) *Gig {
	return &Gig{file: path, RowCreator: creator}
}

func (gig *Gig) Clear() error {
	return DeleteFile(gig.file)
}

func (gig *Gig) Close() error {
	return Close(gig.file)
}

func (gig *Gig) Reconnect() error {
	if gig.db != nil {
		return nil
	}
	db, err := Open(gig.file)
	if err == nil {
		gig.db = db
	}
	return err
}

func (gig *Gig) Count() int {
	if err := gig.Reconnect(); err != nil {
		return -1
	}
	count := gig.db.counterGet("_LEN_KEYS_")
	return int(count)
}

func (gig *Gig) Save(rows ...Row) error {
	if err := gig.Reconnect(); err != nil {
		return err
	}
	var (
		autoIncr        = false
		id       uint32 = 0
		pairs    [][]byte
	)
	for _, row := range rows {
		if autoIncr {
			id++
			row.SetId(id)
		} else {
			id = row.GetId()
			if id == 0 {
				autoIncr = true
				id = uint32(gig.Count()) + 1
				row.SetId(id)
			}
		}
		bins, err := json.Marshal(row)
		if err != nil { //出错，放弃所有
			return err
		}
		pairs = append(pairs, Id2Bin(id))
		pairs = append(pairs, bins)
	}
	return gig.db.sets(pairs)
}

func (gig *Gig) Find(from []byte, limit int, offset int, asc bool) ([]Row, error) {
	if err := gig.Reconnect(); err != nil {
		return nil, err
	}
	if gig.RowCreator == nil {
		return nil, errors.New("There is no RowCreator()")
	}
	var rows []Row
	keys := gig.db.readKeys(from, uint32(limit), uint32(offset), asc)
	for i, bins := range gig.db.gets(keys) {
		if i%2 == 0 {
			continue
		}
		row := gig.RowCreator()
		err := json.Unmarshal(bins, row)
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (gig *Gig) All(limit int, asc bool) ([]Row, error) {
	return gig.Find(nil, limit, 0, asc)
}

func (gig *Gig) Get(key string) (Row, error) {
	if err := gig.Reconnect(); err != nil {
		return nil, err
	}
	if gig.RowCreator == nil {
		return nil, errors.New("There is no RowCreator()")
	}
	bins, err := gig.db.readKey(key)
	if err != nil {
		return nil, err
	}
	row := gig.RowCreator()
	err = json.Unmarshal(bins, row)
	return row, err
}

func (gig *Gig) Set(key string, row Row) error {
	if err := gig.Reconnect(); err != nil {
		return err
	}
	bins, err := json.Marshal(row)
	if err != nil {
		return err
	}
	err = gig.db.setKey(key, bins)
	return err
}

func (gig *Gig) Del(key string) error {
	if err := gig.Reconnect(); err != nil {
		return err
	}
	gig.db.deleteKey(key)
	return nil
}

func (gig *Gig) Has(key string) (bool, error) {
	if err := gig.Reconnect(); err != nil {
		return false, err
	}
	exist := gig.db.has(key)
	return exist, nil
}
