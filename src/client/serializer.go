package client

import (
	"bytes"
	"encoding/json"

	"mrkv/src/common/labgob"
)

type Serializer interface {
	Serialize(val interface{}) ([]byte, error)
}

type StringSerializer struct {
}

func (s *StringSerializer) Serialize(val interface{}) ([]byte, error) {
	str := val.(string)
	return []byte(str), nil
}

type JsonSerializer struct {
}

func (s *JsonSerializer) Serialize(val interface{}) ([]byte, error) {
	return json.Marshal(val)
}

type GobSerializer struct {
}

func (s *GobSerializer) Serialize(val interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	if err := encoder.Encode(val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
