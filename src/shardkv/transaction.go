package shardkv

import (
	"errors"
	"log"
	"sync/atomic"
)

type TxReply struct {
	Err string
}

func (ck *Clerk) ProcessTransaction(ops []Op, delayS ...int) error {
	reqNo := atomic.AddInt32(&ck.reqNumber, 100)
	d := 0
	if len(delayS) > 0 {
		d = delayS[0]
	}
	args := &TxOp{
		Ops:           ops,
		ClientId:      ck.uuid,
		RequestNumber: reqNo,
		Delay:         d,
	}

	// For now, choose a random replica group from the configuration.
	// The configuration's Groups field is a map[int][]string, where the key is the group ID.
	// We'll choose one of the groups randomly.
	// ideally we should pick the one liek spanner does
	var serverList []string
	for gid, servers := range ck.config.Groups {
		log.Printf("selected group %d for being the coordinator", gid)
		serverList = servers
		break // or choose randomly from all groups, here we simply take the first group found
	}
	if len(serverList) == 0 {
		return nil
	}
	// Pick a random server from the chosen group.
	var reply TxReply
	for si := 0; si < len(serverList); si++ {
		srv := ck.make_end(serverList[si])
		ok := srv.Call("ShardKV.ProcessTransaction", args, &reply)
		if ok && reply.Err == OK {
			return nil
		} else if reply.Err == "LockFailed" {
			return errors.New("LockFailed")
		}
	}
	return nil
}
