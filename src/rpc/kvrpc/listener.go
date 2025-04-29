// pkg/rpc/labgobrpc/listener.go
package kvrpc

import (
	"fmt"
	"net"
	"net/rpc"
)

// Serve listens on addr (e.g. ":2379"), registers svc under name, and serves forever.
func Serve(addr, name string, svc interface{}) error {
	// register the service
	if err := rpc.RegisterName(name, svc); err != nil {
		return fmt.Errorf("rpc register: %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// optionally log and continue
			continue
		}
		go rpc.ServeCodec(NewServerCodec(conn))
	}
}
