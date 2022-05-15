package netw

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/Allen1211/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *RPCArgBase) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Gid":
			z.Gid, err = dc.ReadInt()
			if err != nil {
				err = msgp.WrapError(err, "Gid")
				return
			}
		case "Peer":
			z.Peer, err = dc.ReadInt()
			if err != nil {
				err = msgp.WrapError(err, "Peer")
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
func (z RPCArgBase) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Gid"
	err = en.Append(0x82, 0xa3, 0x47, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt(z.Gid)
	if err != nil {
		err = msgp.WrapError(err, "Gid")
		return
	}
	// write "Peer"
	err = en.Append(0xa4, 0x50, 0x65, 0x65, 0x72)
	if err != nil {
		return
	}
	err = en.WriteInt(z.Peer)
	if err != nil {
		err = msgp.WrapError(err, "Peer")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z RPCArgBase) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Gid"
	o = append(o, 0x82, 0xa3, 0x47, 0x69, 0x64)
	o = msgp.AppendInt(o, z.Gid)
	// string "Peer"
	o = append(o, 0xa4, 0x50, 0x65, 0x65, 0x72)
	o = msgp.AppendInt(o, z.Peer)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *RPCArgBase) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Gid":
			z.Gid, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Gid")
				return
			}
		case "Peer":
			z.Peer, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Peer")
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
func (z RPCArgBase) Msgsize() (s int) {
	s = 1 + 4 + msgp.IntSize + 5 + msgp.IntSize
	return
}