// pkg/rpc/labgobrpc/encoder.go
package kvrpc

import (
	"io"

	"6.824/labgob"
)

// Encoder wraps labgob.LabEncoder for network RPC.
type Encoder struct {
	enc *labgob.LabEncoder
}

// NewEncoder returns a new Encoder writing to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{enc: labgob.NewEncoder(w)}
}

// Encode serializes v.
func (e *Encoder) Encode(v interface{}) error {
	return e.enc.Encode(v)
}
