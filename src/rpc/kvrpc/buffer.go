// pkg/rpc/labgobrpc/bufferpool.go
package kvrpc

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// GetBuffer grabs a fresh buffer from the pool.
func GetBuffer() *bytes.Buffer {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

// PutBuffer returns a buffer to the pool.
func PutBuffer(b *bytes.Buffer) {
	bufPool.Put(b)
}
