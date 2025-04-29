// pkg/rpc/dialer.go
package rpc

import (
	"net"
	"net/rpc"
	"time"

	"6.824/rpc/kvrpc"
)

// DialWithTimeout dials the given address over TCP with the specified timeout,
// and returns an *rpc.Client using the labgobrpc ClientCodec.
func DialWithTimeout(addr string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	codec := kvrpc.NewClientCodec(conn)
	return rpc.NewClientWithCodec(codec), nil
}
