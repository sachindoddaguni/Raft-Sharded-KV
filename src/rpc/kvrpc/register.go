// pkg/rpc/labgobrpc/register.go
package kvrpc

import (
	"6.824/labgob"
	"6.824/shardkv"
)

// init runs at program start to register all RPC structs.
func init() {
	labgob.Register(shardkv.Op{})
	labgob.Register(shardkv.ConfigChange{})
	labgob.Register(shardkv.InternalResp{})
	labgob.Register(shardkv.LockArgs{})
	labgob.Register(shardkv.LockReply{})
	labgob.Register(shardkv.UnlockArgs{})
	labgob.Register(shardkv.UnlockReply{})
	labgob.Register(shardkv.TxOp{})
	// add any other custom types here…
}
