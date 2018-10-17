package matcher

import (
	"bufio"
	"bytes"
	"io"
)

//过滤非法法包
type FilterFunc func(chunk []byte) []byte

//继续查找结束标记，确定当前帧是否完整
type SplitAheadFunc func(data []byte, j int) int

//判断匹配开头的方法
type MatchBytesFunc func(data []byte) (int, int)

func MatchToken(token, data []byte) (int, int) {
	var index int
	size := len(token)
	if size == 1 {
		index = bytes.IndexByte(data, token[0])
	} else {
		index = bytes.Index(data, token)
	}
	return index, size
}

//按前后标记拆包
type TokenMatcher struct {
	MatchStart MatchBytesFunc
	MatchEnd   MatchBytesFunc
	Spliter    bufio.SplitFunc
	SplitAhead SplitAheadFunc
}

func NewTokenMatcher(start, end []byte, ahead bool) *TokenMatcher {
	obj := &TokenMatcher{}
	obj.Spliter = obj.CreateSplitFunc()
	if ahead {
		obj.SplitAhead = obj.CreateSplitAhead()
	}
	if start != nil {
		obj.MatchStart = func(data []byte) (int, int) {
			return MatchToken(start, data)
		}
	}
	if end != nil {
		obj.MatchEnd = func(data []byte) (int, int) {
			return MatchToken(end, data)
		}
	}
	return obj
}

//生成解析方法
func (obj *TokenMatcher) CreateSplitFunc() bufio.SplitFunc {
	return func(data []byte, atEOF bool) (int, []byte, error) {
		if i, offset := obj.MatchStart(data); i >= 0 {
			if j, n := obj.MatchEnd(data[offset:]); j > i {
				j += offset
				if !atEOF && obj.SplitAhead != nil {
					j = obj.SplitAhead(data, j)
				}
				//返回一个完整帧，已处理缓存中开头的j+n字节（包含当前帧）
				return j + n, data[i+offset : j], nil
			}
		}
		if atEOF { //到结尾了，缓存中没有更多数据
			return 0, nil, io.EOF
		} else {
			return 0, nil, nil //没找到完整帧，继续往下读
		}
	}
}

//生成结束标记不可靠时，进一步检测的方法
func (obj *TokenMatcher) CreateSplitAhead() SplitAheadFunc {
	//对于消息体中可能含有结束标记的情况，还需要检查下一帧再作判断
	return func(data []byte, j int) int {
		length := len(data) - 1
		for j < length {
			if i, _ := obj.MatchStart(data[j+1:]); i == 0 {
				return j //确认了接下来是下一帧的开头
			}
			k, n := obj.MatchEnd(data[j+1:])
			if k < 0 {
				return j //找不到下一个结束标记，到此为止，避免死循环
			}
			j += k + n
		}
		return j
	}
}

//解析字节流
func (obj *TokenMatcher) SplitStream(reader io.Reader, filter FilterFunc) ([][]byte, error) {
	var output [][]byte
	//defer close(output)
	scanner := bufio.NewScanner(reader)
	scanner.Split(obj.Spliter)
	for scanner.Scan() {
		chunk := filter(scanner.Bytes())
		if chunk != nil {
			output = append(output, chunk)
		}
	}
	return output, scanner.Err()
}
