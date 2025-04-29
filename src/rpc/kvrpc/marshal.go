// pkg/rpc/labgobrpc/marshal.go
package kvrpc

import (
	"bytes"

	"6.824/labgob"
)

// Marshal serializes v into a byte slice using our LabEncoder.
func Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal deserializes data into v using our LabDecoder.
func Unmarshal(data []byte, v interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	return dec.Decode(v)
}
