package helpers

import (
	"bytes"
	"encoding/hex"
)

//16进制的字符表示和二进制字节表示互转
var Bin2Hex = hex.EncodeToString

var Hex2Bin = func(data string) []byte {
	if block, err := hex.DecodeString(data); err == nil {
		return block
	}
	return nil
}

//字符求余转为数字
func ToNum(c byte) uint8 {
	return uint8((c - '0' + 100) % 10)
}

//ASCII字符串和字节组互转
func Str2Bin(s string) []byte {
	return []byte(s)
}

func Bin2Str(b []byte) string {
	return string(b)
}

//多个字节组相连
func Concat(blocks ...[]byte) []byte {
	return bytes.Join(blocks, nil)
}

//补充到指定长度
func Extend(block, chunk []byte, padSize int) []byte {
	times := padSize / len(chunk)
	pads := bytes.Repeat(chunk, times)
	var zeros []byte
	if size := padSize - len(pads); size > 0 {
		zeros = bytes.Repeat([]byte{0x00}, size)
	}
	return Concat(block, pads, zeros)
}

//异或校验，JT/T808的检验码使用此方式
func BlockCheck(block []byte) byte {
	result := byte(0x00)
	for _, bin := range block {
		result ^= bin
	}
	return result
}
