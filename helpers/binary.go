package helpers

import (
	"bytes"
	"encoding/hex"
)

//字符求余转为数字
var ToNum = func(c byte) uint8 {
	return uint8((c - '0' + 100) % 10)
}

//16进制的字符表示和二进制字节表示互转
var Bin2Hex = hex.EncodeToString

var Hex2Bin = func(data string) []byte {
	if block, err := hex.DecodeString(data); err == nil {
		return block
	}
	return nil
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
func Extend(block, pads []byte, padSize int) []byte {
	if pads == nil || padSize <= 0 {
		return block
	}
	times := padSize / len(pads)
	chunk := bytes.Repeat(pads, times)
	var zeros []byte
	if size := padSize - len(chunk); size > 0 {
		zeros = bytes.Repeat([]byte{0x00}, size)
	}
	return Concat(block, chunk, zeros)
}

//异或校验，JT/T808的检验码使用此方式
func BlockCheck(block []byte) byte {
	result := byte(0x00)
	for _, bin := range block {
		result ^= bin
	}
	return result
}
