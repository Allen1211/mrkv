package utils

import (
	"bytes"

	"github.com/Allen1211/msgp/msgp"

	"mrkv/src/common"
)

func MsgpEncode(e msgp.Encodable) []byte {
	buf := new(bytes.Buffer)
	if err := msgp.Encode(buf, e); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func MsgpDecode(data []byte, d msgp.Decodable) {
	buf := bytes.NewReader(data)
	if err := msgp.Decode(buf, d); err != nil {
		panic(err)
	}
}

func EncodeCmdWrap(tp uint8, body []byte) []byte {
	buf := bytes.NewBuffer([]byte{})
	buf.WriteByte(tp)
	buf.Write(body)
	return buf.Bytes()
}

func DecodeCmdWrap(data []byte) common.CmdWrap {
	return common.CmdWrap {
		Type: data[0],
		Body: data[1:],
	}
}