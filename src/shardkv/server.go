package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	GET         = 0
	PUT         = 1
	APPEND      = 2
	LOCK_KEYS   = 3
	UNLOCK_KEYS = 4
	TX_PREPARE  = 5
	TX_COMMIT   = 6
	TX_ABORT    = 7
)

type Op struct {
	Type        int
	Arg1        string
	Arg2        string
	ClientUuid  int64
	ClientReqNo int32
	TxID        string
}

type ConfigChange struct {
	OldConfig           shardctrler.Config
	NewConfig           shardctrler.Config
	ShardKvs            map[string]string
	OldClientReplies    map[int64]InternalResp
	ConfigChangeRequest bool // true means only update the config to new config,
	// false means new shards received
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd
	dead     int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kv                     map[string]string
	lastClientCommandReply map[int64]InternalResp
	ReplyWaitChan          map[string]chan *InternalResp

	lastApplied int

	shardController *shardctrler.Clerk
	config          shardctrler.Config
	lastConfig      shardctrler.Config
	configUpdating  bool
	shardLock       sync.RWMutex

	// locking structures
	lockTable   map[string]string
	lockTableMu sync.Mutex

	ContainerId string
	Port        string

	pendingTxs map[string][]Op
}

type InternalResp struct {
	ReqNumber int32
	Reply     string
	Valid     bool // Valid = false means send ErrOldRequest reply
	Term      int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	kv.shardLock.RLock()
	if kv.inMyShard(args.Key) == false {
		reply.Err = ErrWrongGroup
		kv.shardLock.RUnlock()
		return
	}
	if args.ConfigNumber > kv.config.Num {
		reply.Err = ErrServerNotUpdated
		kv.shardLock.RUnlock()
		return
	}
	kv.shardLock.RUnlock()

	raftCmd := Op{
		Type:        GET,
		Arg1:        args.Key,
		Arg2:        "",
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	kv.mu.Lock()
	idx, term, isLeader := kv.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
		kv.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrWrongGroup
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Value = resp.Reply
			reply.Err = OK
		}
	}
}

func logTransaction(kvPort string, txID string, ops []Op) {
	var b strings.Builder
	// header
	b.WriteString(fmt.Sprintf("=== Transaction %s ===\n", txID))

	// each op on its own line, indented
	for i, op := range ops {
		switch op.Type {
		case GET:
			b.WriteString(fmt.Sprintf("  %2d) GET    key=%q\n", i+1, op.Arg1))
		case PUT:
			b.WriteString(fmt.Sprintf("  %2d) PUT    key=%q, val=%q\n", i+1, op.Arg1, op.Arg2))
		}
	}

	// footer
	b.WriteString("========================\n")

	// send it off
	logToServer(kvPort, b.String(), "yellow")
}

func (kv *ShardKV) ProcessTransaction(args *TxOp, reply *GetReply) {
	// 1) Leader check
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 2) Build txID
	txID := strconv.FormatInt(args.ClientId, 10) + "-" + strconv.Itoa(int(args.RequestNumber))
	logToServer(kv.Port, fmt.Sprintf("tx %s: I am the coordinator on group %d", txID, kv.gid), "yellow")
	logTransaction(kv.Port, txID, args.Ops)

	// 3) Partition keys into local vs remote
	localKeys := make([]string, 0, len(args.Ops))
	remoteKeys := make(map[int][]string)
	for _, op := range args.Ops {
		key := op.Arg1
		shard := key2shard(key)
		gid := kv.config.Shards[shard]
		if gid == kv.gid {
			localKeys = append(localKeys, key)
		} else {
			remoteKeys[gid] = append(remoteKeys[gid], key)
		}
	}

	logToServer(kv.Port, "Starting prepare phase for the transaction", "red")

	var prepareBuf bytes.Buffer
	labgob.NewEncoder(&prepareBuf).Encode(args.Ops)

	prepareOp := Op{
		Type:        TX_PREPARE,
		TxID:        txID,
		Arg2:        prepareBuf.String(),
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	idx, term, _ := kv.rf.Start(prepareOp)
	ch := make(chan *InternalResp, 1)
	kv.mu.Lock()
	kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
	kv.mu.Unlock()

	resp := <-ch
	if !resp.Valid || resp.Term != term {
		reply.Err = TransactionFailed
		return
	}

	// 4) Log the distribution
	var dist strings.Builder
	dist.WriteString(fmt.Sprintf("tx %s key distribution:\n  local keys: %v", txID, localKeys))
	gids := make([]int, 0, len(remoteKeys))
	for gid := range remoteKeys {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for _, gid := range gids {
		dist.WriteString(fmt.Sprintf("\n  remote group %d keys: %v", gid, remoteKeys[gid]))
	}
	logToServer(kv.Port, dist.String(), "red")

	// 5) Lock local keys via RPC to ourselves
	lockedLocal := make([]string, 0, len(localKeys))
	conflictsLocal := make([]string, 0)
	localOK := true

	for _, key := range localKeys {
		args.RequestNumber += 1
		lockArgs := &LockArgs{Keys: []string{key}, TxID: txID, RequestNumber: args.RequestNumber}
		var lockReply LockReply
		// try each replica in our own group until one succeeds
		got := false
		for _, srvName := range kv.config.Groups[kv.gid] {
			srv := kv.make_end(srvName)
			if srv.Call("ShardKV.LockKeys", lockArgs, &lockReply) && lockReply.Err == OK {
				got = true
				lockedLocal = append(lockedLocal, key)
				break
			}
		}
		if !got {
			localOK = false
			conflictsLocal = append(conflictsLocal, fmt.Sprintf("%s locked by other tx", key))
			break
		}
	}

	if !localOK {
		// roll back any local locks we did get
		for _, key := range lockedLocal {
			unlockArgs := &UnlockArgs{Keys: []string{key}, TxID: txID}
			var ur UnlockReply
			for _, srvName := range kv.config.Groups[kv.gid] {
				srv := kv.make_end(srvName)
				if srv.Call("ShardKV.UnlockKeys", unlockArgs, &ur) && ur.Err == OK {
					break
				}
			}
		}
		logToServer(kv.Port, fmt.Sprintf(
			"tx %s: FAILED to lock local keys. successes=%v, conflicts=%v",
			txID, lockedLocal, conflictsLocal,
		), "red")
		reply.Err = "LockFailed"
		return
	}
	logToServer(kv.Port, fmt.Sprintf("tx %s: successfully locked local keys %v", txID, lockedLocal), "red")

	// 6) Lock remote keys
	remoteOK := true
	lockedRemote := make(map[int][]string)
	failedRemote := make(map[int][]string)

	for gid, keys := range remoteKeys {
		args.RequestNumber += 1
		lockArgs := &LockArgs{Keys: keys, TxID: txID, RequestNumber: args.RequestNumber}
		got := false

		for _, srvName := range kv.config.Groups[gid] {
			srv := kv.make_end(srvName)
			var lockReply LockReply
			if srv.Call("ShardKV.LockKeys", lockArgs, &lockReply) && lockReply.Err == OK {
				got = true
				lockedRemote[gid] = keys
				logToServer(kv.Port, fmt.Sprintf(
					"tx %s: locked remote keys %v on group %d",
					txID, keys, gid), "red")
				break
			}
		}
		if !got {
			remoteOK = false
			failedRemote[gid] = keys
			logToServer(kv.Port, fmt.Sprintf(
				"tx %s: FAILED to lock remote keys %v on group %d",
				txID, keys, gid), "red")
			break
		}
	}

	if !remoteOK {
		// roll back local locks
		for _, key := range lockedLocal {
			unlockArgs := &UnlockArgs{Keys: []string{key}, TxID: txID}
			var ur UnlockReply
			for _, srvName := range kv.config.Groups[kv.gid] {
				srv := kv.make_end(srvName)
				if srv.Call("ShardKV.UnlockKeys", unlockArgs, &ur) && ur.Err == OK {
					break
				}
			}
		}
		// roll back any remote locks we did get
		for gid, keys := range lockedRemote {
			unlockArgs := &UnlockArgs{Keys: keys, TxID: txID}
			var ur UnlockReply
			for _, srvName := range kv.config.Groups[gid] {
				srv := kv.make_end(srvName)
				if srv.Call("ShardKV.UnlockKeys", unlockArgs, &ur) && ur.Err == OK {
					break
				}
			}
		}
		reply.Err = "LockFailed"
		return
	}

	logToServer(kv.Port, fmt.Sprintf(
		"tx %s: all keys locked successfully: local=%v remote=%v",
		txID, lockedLocal, lockedRemote,
	), "red")

	// ... now proceed to 2PC prepare / commit ...

	logToServer(kv.Port, fmt.Sprintf("tx %s: Starting COMMIT phase for the transaction", txID), "red")
	if args.Delay > 0 {
		logToServer(kv.Port, fmt.Sprintf("Sleeping with delay %d secs", args.Delay), "blue")
		if args.Delay == 50 {
			kv.Kill()
		}
		time.Sleep(time.Duration(args.Delay) * time.Second)
	}

	commitOK := true

	for _, op := range args.Ops {
		key := op.Arg1
		shard := key2shard(key)
		gid := kv.config.Shards[shard]
		servers := kv.config.Groups[gid]
		if len(servers) == 0 {
			commitOK = false
			logToServer(kv.Port, fmt.Sprintf(
				"tx %s: COMMIT %s %q → FAILED (no group %d)",
				txID, opTypeName(op.Type), key, gid,
			), "blue")
			break
		}

		switch op.Type {
		case GET:
			ga := &GetArgs{
				Key:           key,
				ClientId:      args.ClientId,
				RequestNumber: args.RequestNumber,
				ConfigNumber:  kv.config.Num,
			}
			got := false
			for _, srvName := range servers {
				srv := kv.make_end(srvName)
				var gr GetReply
				if srv.Call("ShardKV.Get", ga, &gr) && (gr.Err == OK || gr.Err == ErrNoKey) {
					logToServer(kv.Port, fmt.Sprintf(
						"tx %s: COMMIT GET %q → %q",
						txID, key, gr.Value,
					), "blue")
					got = true
					break
				}
			}
			if !got {
				commitOK = false
				logToServer(kv.Port, fmt.Sprintf(
					"tx %s: COMMIT GET %q → FAILED",
					txID, key,
				), "blue")
			}

		case PUT, APPEND:
			pa := &PutAppendArgs{
				Key:           key,
				Value:         op.Arg2,
				Op:            map[int]string{PUT: "Put", APPEND: "Append"}[op.Type],
				ClientId:      args.ClientId,
				RequestNumber: args.RequestNumber,
				ConfigNumber:  kv.config.Num,
			}
			done := false
			for _, srvName := range servers {
				srv := kv.make_end(srvName)
				var pr PutAppendReply
				if srv.Call("ShardKV.PutAppend", pa, &pr) && pr.Err == OK {
					logToServer(kv.Port, fmt.Sprintf(
						"tx %s: COMMIT %s %q → OK",
						txID, pa.Op, key,
					), "blue")
					done = true
					break
				}
			}
			if !done {
				commitOK = false
				logToServer(kv.Port, fmt.Sprintf(
					"tx %s: COMMIT %s %q → FAILED",
					txID, pa.Op, key,
				), "blue")
			}

		default:
			commitOK = false
			logToServer(kv.Port, fmt.Sprintf(
				"tx %s: COMMIT UnknownOp(%d) → FAILED",
				txID, op.Type,
			), "blue")
		}

		if !commitOK {
			break
		}
	}

	// 10) Set overall reply.Err

	// 11) Always unlock everything before returning
	allGroups := make(map[int][]string)
	allGroups[kv.gid] = localKeys
	for gid, keys := range remoteKeys {
		allGroups[gid] = keys
	}
	for gid, keys := range allGroups {
		ua := &UnlockArgs{Keys: keys, TxID: txID}
		for _, srvName := range kv.config.Groups[gid] {
			srv := kv.make_end(srvName)
			var ur UnlockReply
			if srv.Call("ShardKV.UnlockKeys", ua, &ur) && ur.Err == OK {
				logToServer(kv.Port, fmt.Sprintf(
					"tx %s: UNLOCK group %d keys %v",
					txID, gid, keys), "blue")
				break
			}
		}
	}
	commitOp := Op{
		Type:        TX_COMMIT,
		TxID:        txID,
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	idx, term, _ = kv.rf.Start(commitOp)
	ch = make(chan *InternalResp, 1)
	kv.mu.Lock()
	kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
	kv.mu.Unlock()
	<-ch
	reply.Err = OK

	if commitOK {
		logToServer(kv.Port, fmt.Sprintf("tx %s: COMMIT phase succeeded", txID), "blue")
	} else {
		logToServer(kv.Port, fmt.Sprintf("tx %s: COMMIT phase failed, rolling back", txID), "blue")
		commitOp := Op{
			Type:        TX_ABORT,
			TxID:        txID,
			ClientUuid:  args.ClientId,
			ClientReqNo: args.RequestNumber,
		}
		idx, term, _ := kv.rf.Start(commitOp)
		ch := make(chan *InternalResp, 1)
		kv.mu.Lock()
		kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
		kv.mu.Unlock()
		<-ch
		reply.Err = "CommitFailed"
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.shardLock.RLock()
	if kv.inMyShard(args.Key) == false {
		reply.Err = ErrWrongGroup
		kv.shardLock.RUnlock()
		return
	}
	if args.ConfigNumber > kv.config.Num {
		reply.Err = ErrServerNotUpdated
		kv.shardLock.RUnlock()
		return
	}
	kv.shardLock.RUnlock()

	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}
	raftCmd := Op{
		Type:        opType,
		Arg1:        args.Key,
		Arg2:        args.Value,
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
		TxID:        args.TxID,
	}

	kv.mu.Lock()
	idx, term, isLeader := kv.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
		kv.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrWrongGroup
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
}

func (kv *ShardKV) abortAllPending() {
	// 1) Snapshot the pendingTxs under lock
	kv.mu.Lock()
	logToServer(kv.Port, fmt.Sprintf("Aborting %d pending transactions...", len(kv.pendingTxs)), "yellow")
	pending := make(map[string][]Op, len(kv.pendingTxs))
	for txID, ops := range kv.pendingTxs {
		pending[txID] = ops
	}
	kv.mu.Unlock()

	// 2) For each tx, unlock via RPC per group
	for txID, ops := range pending {
		// group keys by gid
		keysByGid := make(map[int][]string)
		for _, op := range ops {
			key := op.Arg1
			shard := key2shard(key)
			gid := kv.config.Shards[shard]
			keysByGid[gid] = append(keysByGid[gid], key)
		}

		// issue UnlockKeys RPC to each group
		for gid, keys := range keysByGid {
			args := &UnlockArgs{Keys: keys, TxID: txID}
			for _, srvName := range kv.config.Groups[gid] {
				srv := kv.make_end(srvName)
				var ur UnlockReply
				if srv.Call("ShardKV.UnlockKeys", args, &ur) && ur.Err == OK {
					break
				}
			}
		}

		// 3) Raft‑persist the abort so applyHandler will drop pendingTxs[txID]
		abortOp := Op{Type: TX_ABORT, TxID: txID}
		idx, term, isLeader := kv.rf.Start(abortOp)
		if !isLeader {
			continue
		}
		ch := make(chan *InternalResp, 1)
		kv.mu.Lock()
		kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
		kv.mu.Unlock()
		<-ch
	}
}

func (kv *ShardKV) leaderMonitor() {
	wasLeader := false
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader && !wasLeader {
			// on becoming leader, abort every pending tx
			kv.abortAllPending()
		}
		wasLeader = isLeader
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) handleOp(op Op) (reply string, isValid bool) {

	switch op.Type {
	case TX_PREPARE:
		// decode and buffer every sub‑Op
		var ops []Op
		buf := bytes.NewBufferString(op.Arg2)
		labgob.NewDecoder(buf).Decode(&ops)
		kv.pendingTxs[op.TxID] = ops
		return "", true

	case TX_COMMIT, TX_ABORT:
		delete(kv.pendingTxs, op.TxID)
		return "", true

	case LOCK_KEYS:
		// op.Arg1 = key, op.Arg2 = txID
		key, txID := op.Arg1, op.TxID
		kv.lockTableMu.Lock()
		if owner, held := kv.lockTable[key]; !held || owner == txID {
			// free or already ours
			kv.lockTable[key] = txID
			isValid = true
		} else {
			isValid = false
		}
		kv.lockTableMu.Unlock()
		// reply stays empty
		// record last‐reply for RPC wakeup
		kv.lastClientCommandReply[op.ClientUuid] = InternalResp{
			ReqNumber: op.ClientReqNo,
			Reply:     "",
			Valid:     isValid,
		}
		return "", isValid

	case UNLOCK_KEYS:
		key, txID := op.Arg1, op.TxID
		kv.lockTableMu.Lock()
		if kv.lockTable[key] == txID {
			delete(kv.lockTable, key)
		}
		kv.lockTableMu.Unlock()
		isValid = true
		kv.lastClientCommandReply[op.ClientUuid] = InternalResp{
			ReqNumber: op.ClientReqNo,
			Reply:     "",
			Valid:     true,
		}
		return "", true
	}

	if kv.inMyShard(op.Arg1) {
		// lastClientReply, ok := kv.lastClientCommandReply[op.ClientUuid]
		// if !ok || lastClientReply.ReqNumber < op.ClientReqNo {
		// apply
		if op.Type == GET {
			val, ok := kv.kv[op.Arg1]
			if !ok {
				val = ""
			}
			reply = val
		} else if op.Type == PUT {
			kv.kv[op.Arg1] = op.Arg2
		} else {
			oldVal, ok := kv.kv[op.Arg1]
			if !ok {
				oldVal = ""
			}
			kv.kv[op.Arg1] = oldVal + op.Arg2
		}
		isValid = true
		kv.lastClientCommandReply[op.ClientUuid] = InternalResp{
			ReqNumber: op.ClientReqNo,
			Reply:     reply,
			Valid:     isValid,
		}
		// } else {
		// 	// already applied
		// 	if op.ClientReqNo == lastClientReply.ReqNumber {
		// 		// still have reply
		// 		reply = lastClientReply.Reply
		// 		isValid = true
		// 	} else {
		// 		// old
		// 		isValid = false
		// 	}
		// }
	} else {
		isValid = false
	}
	return reply, isValid
}

func copyConfig(to *shardctrler.Config, from *shardctrler.Config) {
	to.Num = from.Num
	to.Groups = make(map[int][]string)
	for i, sh := range from.Shards {
		to.Shards[i] = sh
	}
	for k, v := range from.Groups {
		to.Groups[k] = v
	}
}

func (kv *ShardKV) handleConfigChange(configChange ConfigChange) bool {

	if configChange.ConfigChangeRequest {
		if kv.configUpdating || configChange.NewConfig.Num <= kv.config.Num {
			return false
		}
		kv.configUpdating = true
		copyConfig(&kv.lastConfig, &kv.config)
		copyConfig(&kv.config, &configChange.NewConfig)
		// log.Printf("group %d server %d: got new configChange %v", kv.gid, kv.me, configChange)
		return true
	} else {
		if kv.configUpdating == false || kv.config.Num != configChange.NewConfig.Num {
			return false
		}
		kv.configUpdating = false
		copyConfig(&kv.lastConfig, &kv.config)
		for k, v := range configChange.ShardKvs {
			kv.kv[k] = v
		}
		for cid, newOldReply := range configChange.OldClientReplies {
			oldOldReply, ok := kv.lastClientCommandReply[cid]
			if !ok || newOldReply.ReqNumber > oldOldReply.ReqNumber {
				kv.lastClientCommandReply[cid] = newOldReply
			}
		}

		// log.Printf("group %d server %d: got new ApplyConfigChange %v", kv.gid, kv.me, configChange)

		return true
	}
}

func opTypeName(t int) string {
	switch t {
	case GET:
		return "GET"
	case PUT:
		return "PUT"
	case APPEND:
		return "APPEND"
	case LOCK_KEYS:
		return "LOCK_KEYS"
	case UNLOCK_KEYS:
		return "UNLOCK_KEYS"
	case TX_PREPARE:
		return "TX_PREPARE"
	case TX_ABORT:
		return "TX_ABORT"
	case TX_COMMIT:
		return "TX_COMMIT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

func (kv *ShardKV) applyHandler() {
	for kv.killed() == false {

		applyMsg := <-kv.applyCh
		// formatted := fmt.Sprintf("group %d, server %d, applymsg %v", kv.gid, kv.me, applyMsg)
		// logToServer(kv.Port, formatted)
		// log.Printf("group %d, server %d, applymsg %v", kv.gid, kv.me, applyMsg)

		if applyMsg.CommandValid {
			if applyMsg.CommandIndex == 0 {
				continue
			}
			op, ok := applyMsg.Command.(Op)
			if ok {
				kv.lastApplied = applyMsg.CommandIndex

				kv.shardLock.Lock()

				reply, isValid := kv.handleOp(op)

				if isValid {
					logToServer(kv.Port, fmt.Sprintf(
						"Server %d: applied Op{%s, %q, %q} at index %d (term %d) — SUCCESS",
						kv.me, opTypeName(op.Type), op.Arg1, op.Arg2,
						applyMsg.CommandIndex, applyMsg.CommandTerm,
					), "green")
				}

				kv.shardLock.Unlock()

				kv.mu.Lock()
				waitChan, ok := kv.ReplyWaitChan[termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex)]
				if ok {
					delete(kv.ReplyWaitChan, termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex))
					kv.mu.Unlock()
					waitChan <- &InternalResp{
						Reply: reply,
						Valid: isValid,
						Term:  applyMsg.CommandTerm,
					}
				} else {
					kv.mu.Unlock()
				}
			} else {
				// config change
				configChange, ok := applyMsg.Command.(ConfigChange)
				raft.Assert(ok, "shardkv : unknown command")
				kv.shardLock.Lock()
				isValid := kv.handleConfigChange(configChange)
				kv.shardLock.Unlock()
				kv.mu.Lock()
				waitChan, ok := kv.ReplyWaitChan[termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex)]
				if ok {
					delete(kv.ReplyWaitChan, termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex))
					kv.mu.Unlock()
					waitChan <- &InternalResp{
						Valid: isValid,
					}

				} else {
					kv.mu.Unlock()
				}
			}

		} else {
			// snapshot
			kv.shardLock.Lock()
			kv.loadSnapShot(applyMsg.Snapshot)
			kv.shardLock.Unlock()
		}
	}
}

func (kv *ShardKV) inMyShard(key string) bool {
	if kv.configUpdating == false {
		return kv.config.Shards[key2shard(key)] == kv.gid
	} else {
		inOldShard := kv.lastConfig.Shards[key2shard(key)] == kv.gid
		inNewShard := kv.config.Shards[key2shard(key)] == kv.gid
		return inOldShard && inNewShard
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	killContainer(kv.ContainerId, "SIGKILL")
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	v := atomic.LoadInt32(&kv.dead)
	return v == 1
}

func (kv *ShardKV) loadSnapShot(snapShot []byte) {
	if snapShot == nil || len(snapShot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	kv.lockTable = make(map[string]string)
	if d.Decode(&kv.lockTable) != nil {
		log.Fatal("shardkv: failed to decode lockTable")
	}

	kv.config = shardctrler.Config{}
	kv.lastConfig = shardctrler.Config{}
	kv.lastClientCommandReply = make(map[int64]InternalResp)
	kv.kv = make(map[string]string)

	if d.Decode(&kv.lastApplied) != nil || // snapshot index
		d.Decode(&kv.kv) != nil || d.Decode(&kv.lastClientCommandReply) != nil ||
		d.Decode(&kv.config) != nil || d.Decode(&kv.lastConfig) != nil ||
		d.Decode(&kv.configUpdating) != nil {
		log.Fatal("kvserver: Failed to restore snapshot")
	}

	// log.Printf("group %d, server %d: loaded snapshot: %v", kv.gid, kv.me, kv.kv)
}

func (kv *ShardKV) stateCompactor() {
	if kv.maxraftstate == -1 {
		return
	}
	for kv.killed() == false {
		// create snapshot
		kv.shardLock.RLock()
		if kv.rf.GetRaftStateSize() >= kv.maxraftstate*2/3 {
			kvCopy := make(map[string]string)
			clientReplyCopy := make(map[int64]InternalResp)
			configCopy := shardctrler.Config{}
			oldConfigCopy := shardctrler.Config{}
			copyConfig(&configCopy, &kv.config)
			copyConfig(&oldConfigCopy, &kv.lastConfig)
			isUpdatingCopy := kv.configUpdating
			for k, v := range kv.kv {
				kvCopy[k] = v
			}
			for k, v := range kv.lastClientCommandReply {
				clientReplyCopy[k] = v
			}
			snapshotIndex := kv.lastApplied // raft should not delete indexes that we haven't applied
			kv.shardLock.RUnlock()

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(snapshotIndex)
			e.Encode(kvCopy)
			e.Encode(clientReplyCopy)
			e.Encode(configCopy)
			e.Encode(oldConfigCopy)
			e.Encode(isUpdatingCopy)
			kv.lockTableMu.Lock()
			e.Encode(kv.lockTable)
			kv.lockTableMu.Unlock()
			snapshot := w.Bytes()
			kv.rf.Snapshot(snapshotIndex, snapshot)
		} else {
			kv.shardLock.RUnlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) configUpdater() {
	for kv.killed() == false {

		kv.shardLock.RLock()
		oldConfig := shardctrler.Config{}
		copyConfig(&oldConfig, &kv.config)
		queryNum := oldConfig.Num + 1
		isUpdating := kv.configUpdating
		kv.shardLock.RUnlock()

		newConfig := kv.shardController.Query(queryNum) // sequentially update configs

		if newConfig.Num > oldConfig.Num || isUpdating {
			go func() {
				if isUpdating == false {
					raftCmd := ConfigChange{
						OldConfig:           oldConfig,
						NewConfig:           newConfig,
						ShardKvs:            nil,
						OldClientReplies:    nil,
						ConfigChangeRequest: true,
					}

					kv.mu.Lock()
					idx, term, isLeader := kv.rf.Start(raftCmd)
					if isLeader == false {
						kv.mu.Unlock()
						return
					} else {
						ch := make(chan *InternalResp, 1)
						kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
						kv.mu.Unlock()
						resp := <-ch
						if resp.Valid == false {
							return
						}
						// config updated
					}
				}

				kv.shardLock.RLock()
				if kv.configUpdating {
					copyConfig(&newConfig, &kv.config)
					copyConfig(&oldConfig, &kv.lastConfig)

					// ask other groups for shards I don't have
					var requiredShards []int
					for shardNum, newGid := range newConfig.Shards {
						oldGid := oldConfig.Shards[shardNum]
						if oldGid == 0 {
							// invalid gid
							continue
						}
						if newGid == kv.gid && oldGid != newGid {
							requiredShards = append(requiredShards, shardNum)
						}
					}
					kv.shardLock.RUnlock()
					newKVs, newOldClientReplies := kv.getNewShards(requiredShards, oldConfig)
					// log.Printf("group %d, server %d: got new kvs %v", kv.gid, kv.me, newKVs)
					raftCmd := ConfigChange{
						OldConfig:           oldConfig,
						NewConfig:           newConfig,
						ShardKvs:            newKVs,
						OldClientReplies:    newOldClientReplies,
						ConfigChangeRequest: false,
					}
					ch := make(chan *InternalResp, 1)
					kv.mu.Lock()
					idx, term, isLeader := kv.rf.Start(raftCmd)
					kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
					if isLeader == false {
						kv.mu.Unlock()
						return
					} else {
						ch = make(chan *InternalResp, 1)
						kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
						kv.mu.Unlock()
						resp := <-ch
						if !resp.Valid {
							return
						}
						// new shards applied
					}
				} else {
					kv.shardLock.RUnlock()
					return
				}
			}()

		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) getNewShards(RequiredShards []int,
	oldConfig shardctrler.Config) (map[string]string, map[int64]InternalResp) {
	// for now sequentially req shards

	newKv := make(map[string]string)
	newClientReplyMap := make(map[int64]InternalResp)
	shardsFromGroup := make(map[int][]int)

	for _, shard := range RequiredShards {
		gid := oldConfig.Shards[shard]
		arr, ok := shardsFromGroup[gid]
		if !ok {
			arr = make([]int, 0)
			shardsFromGroup[gid] = arr
		}
		shardsFromGroup[gid] = append(arr, shard)
	}

	for gid, shards := range shardsFromGroup {
		group, _ := oldConfig.Groups[gid]

		req := SendShardArgs{
			Shards:    shards,
			ConfigNum: oldConfig.Num,
		}
		reply := SendShardReply{}
		kv.sendShardRequest(&req, &reply, group)
		for k, v := range reply.ShardKVs {
			newKv[k] = v
		}
		for cid, newReply := range reply.LastClientResponses {
			oldReply, ok := newClientReplyMap[cid]
			if !ok || newReply.ReqNumber > oldReply.ReqNumber {
				newClientReplyMap[cid] = newReply
			}
		}
	}

	return newKv, newClientReplyMap
}

func (kv *ShardKV) sendShardRequest(args *SendShardArgs, reply *SendShardReply, group []string) {

	servers := make([]*labrpc.ClientEnd, len(group))
	for i, serverName := range group {
		servers[i] = kv.make_end(serverName)
	}

	doneChan := make(chan *SendShardReply, 10)
	leader := 0
	sendReq := func(to int) {
		// log.Printf("group %d, sending req %d to %d", kv.gid, args, to)
		sendShardReply := SendShardReply{}
		ok := servers[to].Call("ShardKV.GetShard", args, &sendShardReply)
		if ok {
			doneChan <- &sendShardReply
		}
	}

	go sendReq(leader)

	for {
		select {
		case sendShardReply := <-doneChan:
			if sendShardReply.Err == ErrServerNotUpdated {
				time.Sleep(time.Millisecond * 200)
			} else if sendShardReply.Err != OK {
				leader = (leader + 1) % len(servers)
				go sendReq(leader)
			} else {
				*reply = *sendShardReply
				return
			}
		case <-time.After(time.Millisecond * 200):
			leader = (leader + 1) % len(servers)
			go sendReq(leader)
		}

	}
}

type SendShardArgs struct {
	Shards    []int
	ConfigNum int // ConfigNum of the expected shards, use -1 to get latest
}

type SendShardReply struct {
	ShardKVs            map[string]string
	LastClientResponses map[int64]InternalResp
	Err                 string
	Gid                 int
}

func (kv *ShardKV) GetShard(args *SendShardArgs, reply *SendShardReply) {
	//defer // log.Printf("gorup %d recived get shard req %v, replied %v, my confignum %v", kv.gid, args, reply, kv.config.Num)
	if _, isLeader := kv.rf.GetState(); isLeader {
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
	}

	kv.shardLock.RLock()
	defer kv.shardLock.RUnlock()
	reply.Gid = kv.gid
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrServerNotUpdated
		return
	}

	reply.ShardKVs = make(map[string]string)
	for k, v := range kv.kv {
		shardNo := key2shard(k)
		toGive := false
		for _, shard := range args.Shards {
			if shard == shardNo {
				toGive = true
			}
		}
		if toGive {
			reply.ShardKVs[k] = v
		}
	}

	reply.LastClientResponses = make(map[int64]InternalResp)
	for k, v := range kv.lastClientCommandReply {
		reply.LastClientResponses[k] = v
	}
	reply.Err = OK
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register(map[string]string{})
	labgob.Register(InternalResp{})
	labgob.Register([]int{})
	labgob.Register(map[int64]InternalResp{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ConfigChange{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.dead = 0
	kv.pendingTxs = make(map[string][]Op)

	kv.applyCh = make(chan raft.ApplyMsg)

	containerName := "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(me) + "-container"
	containerID, hostPort, err := createContainer("shard-kv:latest", containerName)
	if err != nil {
		log.Fatalf("Failed to create shard server container for %s: %v", containerName, err)
	}
	kv.ContainerId = containerID
	kv.Port = hostPort
	log.Printf("Shard server %s container created: ID=%s, hostPort=%s", containerName, containerID, hostPort)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh, hostPort)

	kv.lockTable = make(map[string]string)

	kv.config = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}
	kv.lastConfig = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	kv.configUpdating = false

	for i := 0; i < shardctrler.NShards; i++ {
		kv.config.Shards[i] = 0
		kv.lastConfig.Shards[i] = 0
	}

	kv.shardController = shardctrler.MakeClerk(kv.ctrlers)

	kv.kv = make(map[string]string)
	kv.lastClientCommandReply = make(map[int64]InternalResp)

	kv.ReplyWaitChan = make(map[string]chan *InternalResp)
	kv.lastApplied = 0

	kv.loadSnapShot(persister.ReadSnapshot())
	// log.Printf("group %d starting again", kv.gid)
	for _, entry := range kv.rf.Log {
		if entry.Index == 0 {
			continue
		}
		if entry.Index > kv.rf.LastApplied {
			break
		}
		if entry.Index > kv.lastApplied {
			// loaded from snapshot
			continue
		}
		kv.lastApplied = entry.Index
		op, ok := entry.Command.(Op)
		if ok {
			kv.handleOp(op)
		} else {
			configChange, ok := entry.Command.(ConfigChange)
			raft.Assert(ok, "shardkv : unknown command")
			if kv.config.Num < configChange.NewConfig.Num {
				kv.handleConfigChange(configChange)
			}
		}
	}
	go kv.applyHandler()
	go kv.stateCompactor()
	go kv.configUpdater()
	go kv.leaderMonitor()

	return kv
}

func (kv *ShardKV) LockKeys(args *LockArgs, reply *LockReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var (
		lockedKeys []string
		failedKeys []string
	)
	for _, key := range args.Keys {
		// 1) Build a Raft log entry just for this key
		args.RequestNumber += 1
		op := Op{
			Type:        LOCK_KEYS,
			Arg1:        key,
			TxID:        args.TxID,
			ClientUuid:  args.ClientId,
			ClientReqNo: args.RequestNumber,
		}

		// 2) Submit it to Raft
		kv.mu.Lock()
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			// not the leader → immediate failure for this key
			failedKeys = append(failedKeys, key)
			continue
		}
		// register a channel to catch the apply callback
		ch := make(chan *InternalResp, 1)
		kv.ReplyWaitChan[termIndexToString(term, index)] = ch
		kv.mu.Unlock()

		// 3) Wait for that entry to be applied
		resp := <-ch
		if resp.Valid {
			lockedKeys = append(lockedKeys, key)
		} else {
			failedKeys = append(failedKeys, key)
		}
	}

	if len(failedKeys) == 0 {
		logToServer(kv.Port, fmt.Sprintf(
			"LockKeys[%s]: SUCCESS. locked %v on server %d",
			args.TxID, lockedKeys, kv.me,
		), "yellow")
		reply.Err = OK
	} else {
		logToServer(kv.Port, fmt.Sprintf(
			"LockKeys[%s]: FAILED. locked %v; failed %v on server %d",
			args.TxID, lockedKeys, failedKeys, kv.me,
		), "yellow")
		reply.Err = KeyLocked
	}
}

func (kv *ShardKV) UnlockKeys(args *UnlockArgs, reply *UnlockReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var (
		unlockedKeys []string
		failedKeys   []string
	)

	for _, key := range args.Keys {
		args.RequestNumber += 1
		op := Op{
			Type:        UNLOCK_KEYS,
			Arg1:        key,
			TxID:        args.TxID,
			ClientUuid:  args.ClientId,
			ClientReqNo: args.RequestNumber,
		}

		// 2) Send it through Raft
		kv.mu.Lock()
		if _, exists := kv.lockTable[key]; !exists {
			continue
		}
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			failedKeys = append(failedKeys, key)
			continue
		}
		ch := make(chan *InternalResp, 1)
		kv.ReplyWaitChan[termIndexToString(term, index)] = ch
		kv.mu.Unlock()

		// 3) Wait for commit+apply
		resp := <-ch
		if resp.Valid {
			unlockedKeys = append(unlockedKeys, key)
		} else {
			failedKeys = append(failedKeys, key)
		}
	}

	// 4) Log & reply
	if len(failedKeys) == 0 {
		logToServer(kv.Port, fmt.Sprintf(
			"UnlockKeys[%s]: SUCCESS – unlocked %v on server %d",
			args.TxID, unlockedKeys, kv.me,
		), "yellow")
		reply.Err = OK
	} else {
		logToServer(kv.Port, fmt.Sprintf(
			"UnlockKeys[%s]: PARTIAL FAILURE – unlocked %v; failed %v on server %d",
			args.TxID, unlockedKeys, failedKeys, kv.me,
		), "yellow")
		reply.Err = KeyLocked // or a dedicated ErrUnlockFailed
	}
}
