package common

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/Allen1211/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *CmdWrap) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "Type":
			z.Type, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "Type")
				return
			}
		case "Body":
			z.Body, err = dc.ReadBytes(z.Body)
			if err != nil {
				err = msgp.WrapError(err, "Body")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *CmdWrap) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Type"
	err = en.Append(0x82, 0xa4, 0x54, 0x79, 0x70, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint8(z.Type)
	if err != nil {
		err = msgp.WrapError(err, "Type")
		return
	}
	// write "Body"
	err = en.Append(0xa4, 0x42, 0x6f, 0x64, 0x79)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.Body)
	if err != nil {
		err = msgp.WrapError(err, "Body")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *CmdWrap) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Type"
	o = append(o, 0x82, 0xa4, 0x54, 0x79, 0x70, 0x65)
	o = msgp.AppendUint8(o, z.Type)
	// string "Body"
	o = append(o, 0xa4, 0x42, 0x6f, 0x64, 0x79)
	o = msgp.AppendBytes(o, z.Body)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *CmdWrap) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "Type":
			z.Type, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Type")
				return
			}
		case "Body":
			z.Body, bts, err = msgp.ReadBytesBytes(bts, z.Body)
			if err != nil {
				err = msgp.WrapError(err, "Body")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *CmdWrap) Msgsize() (s int) {
	s = 1 + 5 + msgp.Uint8Size + 5 + msgp.BytesPrefixSize + len(z.Body)
	return
}