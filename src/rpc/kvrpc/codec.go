// pkg/rpc/labgobrpc/codec.go
package kvrpc

import (
	"io"
	"net/rpc"
	"sync"
)

// NewServerCodec returns an rpc.ServerCodec that uses gob over conn.
func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &serverCodec{
		dec:  NewDecoder(conn),
		enc:  NewEncoder(conn),
		conn: conn,
	}
}

// NewClientCodec returns an rpc.ClientCodec that uses gob over conn.
func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &clientCodec{
		dec:  NewDecoder(conn),
		enc:  NewEncoder(conn),
		conn: conn,
	}
}

type serverCodec struct {
	dec  *Decoder
	enc  *Encoder
	conn io.Closer
	mu   sync.Mutex
}

func (c *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.dec.Decode(r)
}

func (c *serverCodec) ReadRequestBody(body interface{}) error {
	if body == nil {
		// Discard into a dummy
		var dummy interface{}
		return c.dec.Decode(&dummy)
	}
	return c.dec.Decode(body)
}

func (c *serverCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.enc.Encode(r); err != nil {
		return err
	}
	return c.enc.Encode(body)
}

func (c *serverCodec) Close() error {
	return c.conn.Close()
}

type clientCodec struct {
	dec  *Decoder
	enc  *Encoder
	conn io.Closer
	mu   sync.Mutex
}

func (c *clientCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.enc.Encode(r); err != nil {
		return err
	}
	return c.enc.Encode(body)
}

func (c *clientCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.dec.Decode(r)
}

func (c *clientCodec) ReadResponseBody(body interface{}) error {
	if body == nil {
		// Discard into a dummy
		var dummy interface{}
		return c.dec.Decode(&dummy)
	}
	return c.dec.Decode(body)
}

func (c *clientCodec) Close() error {
	return c.conn.Close()
}
