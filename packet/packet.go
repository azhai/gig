package packet

import (
	"github.com/azhai/gig/helpers"
	"github.com/azhai/gig/packet/matcher"
)

//字节包
type Packet struct {
	payload  []byte
	tuple    *NamedTuple
	filter   matcher.FilterFunc
	Escape   func([]byte) []byte
	Unescape func([]byte) []byte
	Check    func([]byte) bool
}

func NewPacket(tuple *NamedTuple) *Packet {
	return &Packet{tuple: tuple}
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
	if obj.tuple != nil {
		_, least = obj.tuple.Slicer.GetLeastSize()
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
		return chunk
	}
	return obj.filter
}

func (obj *Packet) GetRest() []byte {
	if obj.tuple == nil {
		return nil
	}
	field := obj.tuple.GetRest()
	return obj.GetByField(field)
}

func (obj *Packet) GetByField(field *Field) []byte {
	var total = len(obj.payload)
	if total == 0 {
		return nil
	}
	start, stop := obj.tuple.GetRange(field, total)
	if 0 <= start && start < stop && stop <= total {
		return obj.payload[start:stop]
	}
	return nil
}

func (obj *Packet) GetByName(name string) []byte {
	if obj.tuple == nil {
		return nil
	}
	field := obj.tuple.GetField(name)
	if field == nil {
		return nil
	}
	return obj.GetByField(field)
}

func (obj *Packet) FillField(name string, chunk, pads []byte, total int) (int, int, []byte) {
	field := obj.tuple.GetField(name)
	if field == nil {
		return 0, 0, nil
	}
	start, stop := obj.tuple.GetRange(field, total)
	if start < 0 || stop > total {
		return 0, 0, nil
	}
	rangeSize := stop - start
	padSize := rangeSize - len(chunk)
	if rangeSize <= 0 || padSize < 0 {
		return 0, 0, nil
	}
	if padSize > 0 && pads != nil {
		chunk = helpers.Extend(chunk, pads, padSize)
	}
	return start, stop, chunk
}

func (obj *Packet) SetByName(name string, chunk, pads []byte) int {
	if obj.tuple == nil {
		return 0
	}
	var start, stop int
	var total = len(obj.payload)
	start, stop, chunk = obj.FillField(name, chunk, pads, total)
	return copy(obj.payload[start:stop], chunk)
}
