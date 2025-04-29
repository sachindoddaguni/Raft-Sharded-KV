package kvrpc

import (
	"crypto/tls"
	"net/rpc"
)

// ServeTLS is like Serve but uses TLS credentials.
func ServeTLS(addr, name string, svc interface{}, config *tls.Config) error {
	if err := rpc.RegisterName(name, svc); err != nil {
		return err
	}
	ln, err := tls.Listen("tcp", addr, config)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeCodec(NewServerCodec(conn))
	}
}

// DialTLS connects to a TLS‐enabled RPC server.
/* func DialTLS(addr string, config *tls.Config) (*rpc.Client, error) {
	conn, err := tls.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	return NewClientCodec(), nil
} */
