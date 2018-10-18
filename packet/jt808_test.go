package packet

import (
	"bytes"
	"testing"

	"github.com/azhai/gig/helpers"
	"github.com/azhai/gig/packet/matcher"
)

const (
	//鉴权、心跳、位置上报
	Test_Auth = "7E01020006014530399195003F717361757468797E"
	Test_Beat = "7E0002000001453039919500410A7E"
	Test_GEO  = "7E0200003A0145303991950047000000000000000001CE79700727F388000000000000170509173813010400000000EB16000C00B2898602B41116C096483900060089FFFFFFFFDC7E"
)

func CreateJt808() *Packet {
	parser := NewNamedTuple()
	parser.AddFixeds(2, "cmd", "props")
	parser.AddFixeds(6, "mobile")
	parser.AddFixeds(2, "msgno")
	parser.AddFixeds(-1, "check")

	pkt := NewPacket(parser)
	pkt.Escape = func(data []byte) []byte {
		data = bytes.Replace(data, []byte{0x7d}, []byte{0x7d, 0x01}, -1)
		return bytes.Replace(data, []byte{0x7e}, []byte{0x7d, 0x02}, -1)
	}
	pkt.Unescape = func(data []byte) []byte {
		data = bytes.Replace(data, []byte{0x7d, 0x02}, []byte{0x7e}, -1)
		return bytes.Replace(data, []byte{0x7d, 0x01}, []byte{0x7d}, -1)
	}
	pkt.Check = func(data []byte) bool {
		return helpers.BlockCheck(data) == byte(0x00)
	}
	return pkt
}

func TestReadJT808(t *testing.T) {
	pkt := CreateJt808()
	mach := matcher.NewTokenMatcher([]byte{0x7e}, []byte{0x7e}, false)
	data := helpers.Hex2Bin(Test_Auth + Test_Beat + Test_GEO)
	buffer := bytes.NewReader(data)
	chunks, err := mach.SplitStream(buffer, pkt.GetFilter())
	if err != nil {
		t.Error(err)
		return
	}
	for _, payload := range chunks {
		t.Log(payload)
		pkt.SetPayload(payload, false)
		mobile := pkt.GetByName("mobile")
		t.Log(helpers.Bin2Hex(mobile))
	}
	return
}
