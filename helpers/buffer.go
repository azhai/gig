package helpers

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	FILE_MODE = 0666
	DIR_MODE  = 0777
)

var BufferPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func NewBuffer(size int) *bytes.Buffer {
	buf := BufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	if size > 0 {
		buf.Grow(size)
	}
	return buf
}

func BufAddNum(buf *bytes.Buffer, data ...interface{}) error {
	var err error
	if buf == nil {
		return errors.New("Give me an exist buffer object !")
	}
	for _, one := range data {
		err = binary.Write(buf, binary.BigEndian, one)
		if err != nil {
			break
		}
	}
	return err
}

func BufGetBytes(buf *bytes.Buffer) []byte {
	BufferPool.Put(buf)
	return buf.Bytes()
}

func TouchFile(path string) (bool, error) {
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

func ReadLines(rd io.Reader) []string {
	var (
		lines []string
		line  string
		err   error
	)
	buffer := bufio.NewReader(rd)
	for err == nil {
		line, err = buffer.ReadString('\n')
		if err != nil && err != io.EOF {
			return lines
		}
		line = strings.TrimRight(line, "\r\n")
		//去掉文件最后的空行
		if err != io.EOF || line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
