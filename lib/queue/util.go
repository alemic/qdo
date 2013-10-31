package queue

import (
	"bytes"
	"encoding/gob"
)

func GobEncode(v interface{}) ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func GobDecode(buf []byte, res interface{}) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(res)
	if err != nil {
		return err
	}
	return nil
}
