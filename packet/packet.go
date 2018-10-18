package packet

import (
	"github.com/azhai/gig/helpers"
	"github.com/azhai/gig/packet/matcher"
)

//字节包
type Packet struct {
	payload  []byte
	parser   *NamedTuple
	filter   matcher.FilterFunc
	Escape   func([]byte) []byte
	Unescape func([]byte) []byte
	Check    func([]byte) bool
}

func NewPacket(parser *NamedTuple) *Packet {
	return &Packet{parser: parser}
}

func (obj *Packet) SetPayload(payload []byte, isEscaped bool) {
	if isEscaped && obj.Unescape != nil {
		payload = obj.Unescape(payload)
	}
	obj.payload = payload
}

func (obj *Packet) Serialize(needEscaped bool) []byte {
	payload := obj.payload
	if payload == nil {
		return nil
	}
	if needEscaped && obj.Escape != nil {
		payload = obj.Escape(payload)
	}
	return payload
}

//过滤方法
func (obj *Packet) GetFilter() matcher.FilterFunc {
	if obj.filter != nil {
		return obj.filter
	}
	var least = 1
	if obj.parser != nil {
		least = obj.parser.Slicer.GetLeastSize()
	}
	obj.filter = func(chunk []byte) []byte {
		if len(chunk) < least {
			return nil
		}
		if obj.Unescape != nil {
			chunk = obj.Unescape(chunk)
		}
		if obj.Check != nil && !obj.Check(chunk) {
			return nil
		}
		obj.SetPayload(chunk, false)
		return chunk
	}
	return obj.filter
}

func (obj *Packet) GetRange(name string) (int, int) {
	var length = len(obj.payload)
	if length == 0 || obj.parser == nil {
		return 0, 0
	}
	var field *Field
	if name == "" || name == "<REST>" {
		field = obj.parser.GetRest()
	} else {
		field = obj.parser.GetField(name)
	}
	if field == nil {
		return 0, 0
	}
	return obj.parser.GetRealRange(field, length)
}

func (obj *Packet) GetByName(name string) []byte {
	start, stop := obj.GetRange(name)
	if start < stop {
		return obj.payload[start:stop]
	}
	return nil
}

func (obj *Packet) SetByName(name string, chunk, pads []byte) int {
	start, stop := obj.GetRange(name)
	rangeSize := stop - start
	padSize := rangeSize - len(chunk)
	if rangeSize <= 0 || padSize < 0 {
		return 0
	}
	if padSize > 0 && pads != nil {
		chunk = helpers.Extend(chunk, pads, padSize)
	}
	return copy(obj.payload[start:stop], chunk)
}
