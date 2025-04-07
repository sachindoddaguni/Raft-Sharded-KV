package shardkv

import (
	"crypto/rand"
	"math/big"
	"strconv"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrServerNotUpdated = "ErrServerNotUpdated"
	ErrNoGroupAvailable = "ErrNoGroupAvailable"
	KeyLocked           = "KeyLocked"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	ClientId      int64
	RequestNumber int32
	ConfigNumber  int
}

type PutAppendReply struct {
	Err Err
}

type TxOp struct {
	Ops           []Op
	ClientId      int64
	RequestNumber int32
}

type GetArgs struct {
	Key           string
	ClientId      int64
	RequestNumber int32
	ConfigNumber  int
}

type GetReply struct {
	Err   Err
	Value string
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func termIndexToString(term int, index int) string {
	return strconv.Itoa(term) + "." + strconv.Itoa(index)
}

type LockArgs struct {
	Keys          []string
	TxID          string
	ClientId      int64
	RequestNumber int32
}

type LockReply struct {
	Err string
}

type UnlockArgs struct {
	Keys          []string
	TxID          string // The transaction that wants to release the locks.
	ClientId      int64
	RequestNumber int32
}

type UnlockReply struct {
	Err string
}
