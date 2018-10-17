// package gig implements a low-level key/value store in pure Go.
// Keys stored in memory, Value stored on disk
package gig

import (
	"bytes"
	"context"
	"encoding/binary"
	"io/ioutil"
	"os"
	"sort"
	"time"
)

// Cmd - struct with commands stored in keys
type Cmd struct {
	Seek    uint32
	Size    uint32
	KeySeek uint32
}

// writeAtPos store bytes to file
// if pos<0 store at the end of file
// if withSync == true - do sync on write
func writeAtPos(f *os.File, b []byte, pos int64, withSync bool) (seek int64, n int, err error) {
	seek = pos
	if pos < 0 {
		seek, err = f.Seek(0, 2)
		if err != nil {
			return seek, 0, err
		}
	}
	n, err = f.WriteAt(b, seek)
	if err != nil {
		return seek, n, err
	}
	if withSync {
		return seek, n, f.Sync() // ensure that the write is done.
	}
	return seek, n, err
}

// writeKey create buffer and store key with val address and size
func writeKey(fk *os.File, t uint8, seek, size uint32, key []byte, sync bool, keySeek int64) (newSeek int64, err error) {
	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	buf.Grow(16 + len(key))

	//encode
	binary.Write(buf, binary.BigEndian, uint8(0))                  //1byte version
	binary.Write(buf, binary.BigEndian, t)                         //1byte command code(0-set,1-delete)
	binary.Write(buf, binary.BigEndian, seek)                      //4byte seek
	binary.Write(buf, binary.BigEndian, size)                      //4byte size
	binary.Write(buf, binary.BigEndian, uint32(time.Now().Unix())) //4byte timestamp
	binary.Write(buf, binary.BigEndian, uint16(len(key)))          //2byte key size
	buf.Write(key)                                                 //key

	if sync {
		if keySeek < 0 {
			newSeek, _, err = writeAtPos(fk, buf.Bytes(), int64(-1), true) //fk.Write(buf.Bytes())
		} else {
			newSeek, _, err = writeAtPos(fk, buf.Bytes(), int64(keySeek), true) //fk.WriteAt(buf.Bytes(), int64(keySeek))
		}

	} else {
		newSeek, _, err = writeAtPos(fk, buf.Bytes(), int64(-1), false) //fk.WriteNoSync(buf.Bytes())
	}

	return newSeek, err
}

func writeKeyVal(fk *os.File, fv *os.File, readKey string, writeVal []byte, exists bool, oldCmd *Cmd) (cmd *Cmd, err error) {

	var seek, newSeek int64
	cmd = &Cmd{Size: uint32(len(writeVal))}
	if exists {
		// key exists
		cmd.Seek = oldCmd.Seek
		cmd.KeySeek = oldCmd.KeySeek
		if oldCmd.Size >= uint32(len(writeVal)) {
			//write at old seek new value
			_, _, err = writeAtPos(fv, writeVal, int64(oldCmd.Seek), true)
		} else {
			//write at new seek (at the end of file)
			seek, _, err = writeAtPos(fv, writeVal, int64(-1), true)
			cmd.Seek = uint32(seek)
		}
		if err == nil {
			// if no error - store key at KeySeek
			newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(readKey), true, int64(cmd.KeySeek))
			cmd.KeySeek = uint32(newSeek)
		}
	} else {
		// new key
		// write value at the end of file
		seek, _, err = writeAtPos(fv, writeVal, int64(-1), true)
		cmd.Seek = uint32(seek)
		if err == nil {
			newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, []byte(readKey), true, -1)
			cmd.KeySeek = uint32(newSeek)
		}
	}
	return cmd, err
}

// run read keys from *.idx store and run listeners
func run(parentCtx context.Context, fk *os.File, fv *os.File,
	readRequests <-chan readRequest, writeRequests <-chan writeRequest,
	deleteRequests <-chan deleteRequest, keysRequests <-chan keysRequest,
	setsRequests <-chan setsRequest, getsRequests <-chan getsRequest,
	hasRequests <-chan hasRequest,
	counterGetRequests <-chan counterGetRequest, counterSetRequests <-chan counterSetRequest) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// valDict map with key and address of values
	valDict := make(map[string]*Cmd)
	// keysDict store ordered slice of keys
	var keysDict = make([][]byte, 0)
	// countersDict store counters
	countersDict := make(map[string]uint64)

	//delete key from slice keysDict
	deleteFromKeys := func(b []byte) {
		found := sort.Search(len(keysDict), func(i int) bool {
			return bytes.Compare(keysDict[i], b) >= 0
		})
		if found < len(keysDict) {
			//fmt.Printf("found:%d key:%+v keys:%+v\n", found, b, keysDict)
			if bytes.Equal(keysDict[found], b) {
				keysDict = append(keysDict[:found], keysDict[found+1:]...)
			}
		}
	}

	//appendAsc insert key in slice in ascending order
	appendAsc := func(b []byte) {
		keysLen := len(keysDict)
		found := sort.Search(keysLen, func(i int) bool {
			return bytes.Compare(keysDict[i], b) >= 0
		})
		if found == 0 {
			//prepend
			keysDict = append([][]byte{b}, keysDict...)

		} else {
			if found >= keysLen {
				//not found - postpend ;)
				keysDict = append(keysDict, b)
			} else {
				//found
				//https://blog.golang.org/go-slices-usage-and-internals
				keysDict = append(keysDict, nil)           //grow origin slice capacity if needed
				copy(keysDict[found+1:], keysDict[found:]) //ha-ha, lol, 20x faster
				keysDict[found] = b
			}
		}
	}
	//read keys
	//get buf from pool
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	b, _ := ioutil.ReadAll(fk) //fk.ReadFile()
	buf.Write(b)
	var readSeek uint32
	for buf.Len() > 0 {
		_ = uint8(buf.Next(1)[0]) //format version
		t := uint8(buf.Next(1)[0])
		seek := binary.BigEndian.Uint32(buf.Next(4))
		size := binary.BigEndian.Uint32(buf.Next(4))
		_ = buf.Next(4) //time
		sizeKey := int(binary.BigEndian.Uint16(buf.Next(2)))
		key := buf.Next(sizeKey)
		strkey := string(key)
		cmd := &Cmd{
			Seek:    seek,
			Size:    size,
			KeySeek: readSeek,
		}
		readSeek += uint32(16 + sizeKey)
		switch t {
		case 0:
			if _, exists := valDict[strkey]; !exists {
				//write new key at keys store
				//keysDict = append(keysDict, key)
				appendAsc(key)
			}
			valDict[strkey] = cmd
		case 1:
			delete(valDict, strkey)
			deleteFromKeys(key)
		}
	}
	/*
		for k, v := range valDict {
			fmt.Printf("%+v:%+v\n", k, v)
		}
	*/

	for {
		select {
		case <-ctx.Done():
			// start on Close()
			fk.Close()
			fv.Close()
			//fmt.Println("done")
			return nil
		case dr := <-deleteRequests:
			//fmt.Println("del")
			//for _, v := range valDict {
			//fmt.Printf("%+v\n", v)
			//}
			delete(valDict, dr.deleteKey)
			deleteFromKeys([]byte(dr.deleteKey))
			// delete command append to the end of keys file
			writeKey(fk, 1, 0, 0, []byte(dr.deleteKey), true, -1)
			close(dr.responseChan)
		case wr := <-writeRequests:
			oldCmd, exists := valDict[wr.readKey]
			cmd, err := writeKeyVal(fk, fv, wr.readKey, wr.writeVal, exists, oldCmd)
			if !exists {
				appendAsc([]byte(wr.readKey))
			}
			if err == nil {
				//fmt.Printf("wr:%s %+v\n", wr.readKey, cmd)
				// store command if no error
				valDict[wr.readKey] = cmd
			}
			wr.responseChan <- writeResponse{err}
		case rr := <-readRequests:
			if val, exists := valDict[rr.readKey]; exists {
				//fmt.Printf("rr:%s %+v\n", rr.readKey, val)
				b := make([]byte, val.Size)
				_, err := fv.ReadAt(b, int64(val.Seek))
				rr.responseChan <- readResponse{b, err}
			} else {
				// if no key return eror
				rr.responseChan <- readResponse{nil, ErrKeyNotFound}
			}

		case kr := <-keysRequests:
			var result [][]byte
			result = make([][]byte, 0)
			lenKeys := len(keysDict)
			var start, end, found int
			var byPrefix bool
			found = -1
			if kr.fromKey != nil {
				if bytes.Equal(kr.fromKey[len(kr.fromKey)-1:], []byte("*")) {
					byPrefix = true
					_ = byPrefix
					kr.fromKey = kr.fromKey[:len(kr.fromKey)-1]
					//fmt.Println(string(kr.fromKey))
				}
				found = sort.Search(lenKeys, func(i int) bool {
					//bynary search may return not eq result
					return bytes.Compare(keysDict[i], kr.fromKey) >= 0
				})
				if !kr.asc && byPrefix {
					//iterate if desc and by prefix
					found = lenKeys
					for j := lenKeys - 1; j >= 0; j-- {
						if len(keysDict[j]) >= len(kr.fromKey) {
							if bytes.Equal(keysDict[j][:len(kr.fromKey)], kr.fromKey) {
								found = j
								break
								//fmt.Println("found:", found, string(keysDict[j][:len(kr.fromKey)]), string(keysDict[j]))
							}
						}
					}
				}
				if found == lenKeys {
					//not found
					found = -1
				} else {
					//found
					if !byPrefix && !bytes.Equal(keysDict[found], kr.fromKey) {
						found = -1 //not eq
					}
				}
				// if not found - found will == len and return empty array
				//fmt.Println(string(kr.fromKey), found)
			}
			// ascending order
			if kr.asc {
				start = 0
				if kr.fromKey != nil {
					if found == -1 {
						start = lenKeys
					} else {
						start = found + 1
						if byPrefix {
							//include
							start = found
						}
					}
				}
				if kr.offset > 0 {
					start += int(kr.offset)
				}
				end = lenKeys
				if kr.limit > 0 {
					end = start + int(kr.limit)
					if end > lenKeys {
						end = lenKeys
					}
				}
				if start < lenKeys {
					for i := start; i < end; i++ {
						if byPrefix {
							if len(keysDict[i]) < len(kr.fromKey) {
								break
							} else {
								//compare with prefix
								//fmt.Println("prefix", string(keysDict[i][:len(kr.fromKey)]), string(kr.fromKey))
								if !bytes.Equal(keysDict[i][:len(kr.fromKey)], kr.fromKey) {
									break
								}
							}
						}
						result = append(result, keysDict[i])
					}
				}
			} else {
				//descending
				start = lenKeys - 1
				if kr.fromKey != nil {
					if found == -1 {
						start = -1
					} else {
						start = found - 1
						if byPrefix {
							//include
							start = found
						}
					}
				}

				if kr.offset > 0 {
					start -= int(kr.offset)
				}
				end = 0
				if kr.limit > 0 {
					end = start - int(kr.limit) + 1
					if end < 0 {
						end = 0
					}
				}
				if start >= 0 {
					for i := start; i >= end; i-- {
						if byPrefix {
							if len(keysDict[i]) < len(kr.fromKey) {
								break
							} else {
								//compare with prefix
								//fmt.Println("prefix", string(keysDict[i][:len(kr.fromKey)]), string(kr.fromKey))
								if !bytes.Equal(keysDict[i][:len(kr.fromKey)], kr.fromKey) {
									break
								}
							}
						}
						result = append(result, keysDict[i])
					}
				}
			}
			kr.responseChan <- keysResponse{keys: result}
			close(kr.responseChan)
		case sr := <-setsRequests:
			var err error
			var seek, newSeek int64
			for i := range sr.pairs {
				if i%2 != 0 {
					// on odd - append val and store key
					if sr.pairs[i] == nil || sr.pairs[i-1] == nil {
						break
					}
					//key - sr.pairs[i-1]
					//val - sr.pairs[i]
					cmd := &Cmd{Size: uint32(len(sr.pairs[i]))}
					seek, _, err = writeAtPos(fv, sr.pairs[i], int64(-1), false) //fv.WriteNoSync(sr.pairs[i])
					cmd.Seek = uint32(seek)
					if err != nil {
						break
					}

					newSeek, err = writeKey(fk, 0, cmd.Seek, cmd.Size, sr.pairs[i-1], false, -1)
					cmd.KeySeek = uint32(newSeek)
					if err != nil {
						break
					}
					keyStr := string(sr.pairs[i-1])
					if _, exists := valDict[keyStr]; !exists {
						//write new key at keys store
						appendAsc(sr.pairs[i-1])
						//keysDict = append(keysDict, sr.pairs[i-1])
					}
					valDict[keyStr] = cmd

				}
			}
			if err == nil {
				err = fk.Sync()
				if err == nil {
					err = fv.Sync()
				}
			}

			sr.responseChan <- setsResponse{err}
		case gr := <-getsRequests:
			var result [][]byte
			result = make([][]byte, 0)
			for _, key := range gr.keys {
				if val, exists := valDict[string(key)]; exists {
					//val, _ := fv.Read(int64(val.Size), int64(val.Seek))
					b := make([]byte, val.Size)
					fv.ReadAt(b, int64(val.Seek))
					result = append(result, key)
					result = append(result, b)
				}
			}
			gr.responseChan <- getsResponse{result}
		case hr := <-hasRequests:
			_, exists := valDict[hr.key]
			hr.responseChan <- hasResponse{exists: exists}
		case cgr := <-counterGetRequests:
			var val uint64
			switch cgr.key {
			case NAME_COUNT_KEYS:
				val = uint64(len(keysDict))
			default:
				val, _ = countersDict[cgr.key]
				val++
				countersDict[cgr.key] = val
			}

			cgr.responseChan <- counterGetResponse{counter: val}
		case csr := <-counterSetRequests:
			if csr.store {
				for k, v := range countersDict {
					//store current counter
					//fmt.Printf("%+v:%+v\n", k, v)
					oldCmd, exists := valDict[k]
					if v > 0 {

						buf := make([]byte, 8)
						binary.BigEndian.PutUint64(buf, v)

						writeKeyVal(fk, fv, k, buf, exists, oldCmd)
					}
				}
			} else {
				countersDict[csr.key] = csr.counter
			}

			close(csr.responseChan)
		}

	}
}
