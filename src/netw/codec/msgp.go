package codec

import (
	"bytes"
	"log"

	"github.com/Allen1211/msgp/msgp"

)

type MsgpCodec struct {
}

func (c *MsgpCodec) Decode(data []byte, i interface{}) error {
	d, ok := i.(msgp.Decodable)
	if !ok {
		log.Panicf("%v is not decodable", i)
	}
	return msgp.Decode(bytes.NewReader(data), d)
}

func (c *MsgpCodec) Encode(i interface{}) ([]byte, error) {
	e, ok := i.(msgp.Encodable)
	if !ok {
		log.Panicf("%v is not encodable", i)
	}
	buf := new(bytes.Buffer)
	if err := msgp.Encode(buf, e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil

}
