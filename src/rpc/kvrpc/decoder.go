// pkg/rpc/labgobrpc/decoder.go
package kvrpc

import (
	"io"

	"6.824/labgob"
)

// Decoder wraps labgob.LabDecoder for network RPC.
type Decoder struct {
	dec *labgob.LabDecoder
}

// NewDecoder returns a new Decoder reading from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{dec: labgob.NewDecoder(r)}
}

// Decode deserializes into v.
func (d *Decoder) Decode(v interface{}) error {
	return d.dec.Decode(v)
}
