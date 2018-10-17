
**Description**

Lighty object store in golang.

Inspired by [slowpoke](https://github.com/recoilme/slowpoke)

![slowpoke](http://tggram.com/media/recoilme/photos/file_488344.jpg)

Package slowpoke is a simple key/value store written using Go's standard library only. Keys are stored in memory (with persistence), values stored on disk.

Description on russian: https://habr.com/post/354224/

**Example for gig**

```golang
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/azhai/gig"
)

type Device struct {
	Id   uint32
	IMEI string
}

func (this *Device) GetId() uint32 {
	return 0
}

func (this *Device) SetId(id uint32) error {
	this.Id = id
	return nil
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

var (
	imeiFile string //imei文件
)

func init() {
	flag.StringVar(&imeiFile, "i", "", "imei文件")
	flag.Parse()
}

func main() {
	g := gig.NewGig("imei", func() gig.Row {
		return &Device{}
	})
	defer g.Close()

	var devs []gig.Row
	if imeiFile != "" { //写入
		file, err := os.OpenFile(imeiFile, os.O_RDWR | os.O_CREATE, 0666)
		if err == nil {
			for _, imei := range ReadLines(file) {
				d := &Device{IMEI: imei}
				devs = append(devs, d)
			}
			g.Clear()
			g.Save(devs...)
		}
	}
	if len(devs) == 0 { //读取
		var err error
		devs, err = g.All(5, true)
		if err != nil {
			fmt.Println(err)
		}
	}

	size := len(devs)
	fmt.Println(size)
	if size > 0 {
		fmt.Println(devs[0])
	}
}
```
