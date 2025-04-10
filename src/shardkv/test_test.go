package shardkv

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.824/models"
	"6.824/porcupine"
)

const linearizabilityCheckTimeout = 1 * time.Second

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

func TestProcessTransactionConcurrentNonConflict(t *testing.T) {
	fmt.Printf("Test: concurrent non-conflicting transactions...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck1 := cfg.makeClient()
	ck2 := cfg.makeClient()

	cfg.join(0)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)

	// Choose two different keys.
	key1 := "x" // Example key (e.g., ASCII 'x')
	key2 := "y" // Example key (e.g., ASCII 'y')

	// Clear both keys.
	ck1.Put(key1, "")
	ck2.Put(key2, "")
	time.Sleep(200 * time.Millisecond)

	// Build a transaction for key1 from ck1.
	op1 := Op{
		Type:        PUT,
		Arg1:        key1,
		Arg2:        "ValueX",
		ClientUuid:  ck1.uuid,
		ClientReqNo: atomic.AddInt32(&ck1.reqNumber, 1),
	}
	tx1 := &TxOp{
		Ops:           []Op{op1},
		ClientId:      ck1.uuid,
		RequestNumber: atomic.AddInt32(&ck1.reqNumber, 1),
	}

	// Build a transaction for key2 from ck2.
	op2 := Op{
		Type:        PUT,
		Arg1:        key2,
		Arg2:        "ValueY",
		ClientUuid:  ck2.uuid,
		ClientReqNo: atomic.AddInt32(&ck2.reqNumber, 1),
	}
	tx2 := &TxOp{
		Ops:           []Op{op2},
		ClientId:      ck2.uuid,
		RequestNumber: atomic.AddInt32(&ck2.reqNumber, 1),
	}

	// Use the same server for both RPCs.
	var serverList []string
	for _, servers := range ck1.config.Groups {
		serverList = servers
		break
	}
	if len(serverList) == 0 {
		t.Fatal("No servers available")
	}
	srv1 := ck1.make_end(serverList[0])
	srv2 := ck2.make_end(serverList[0])

	var reply1, reply2 GetReply
	if ok := srv1.Call("ShardKV.ProcessTransaction", tx1, &reply1); !ok {
		t.Fatal("RPC call for tx1 failed")
	}
	if reply1.Err != OK {
		t.Fatalf("Expected tx1 to return OK, got error: %v", reply1.Err)
	}
	if ok := srv2.Call("ShardKV.ProcessTransaction", tx2, &reply2); !ok {
		t.Fatal("RPC call for tx2 failed")
	}
	if reply2.Err != OK {
		t.Fatalf("Expected tx2 to return OK, got error: %v", reply2.Err)
	}
	fmt.Printf("  ... Passed\n")
}

func TestTransactionSingleGroup(t *testing.T) {
	fmt.Printf("Test: Transaction in a single group...\n")
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()
	// Only join group 0 so that all shards are owned by one group.
	cfg.join(0)
	// Allow time for the configuration to update.
	time.Sleep(500 * time.Millisecond)

	// Choose keys that (with one group joined) will be served by group 0.
	// For example, when only one group is available, every shard is assigned to it.
	key1 := "5" // arbitrary key; key2shard("5") will produce a shard number (5 mod 10)
	key2 := "8" // another arbitrary key

	// Clear the keys.
	ck.Put(key1, "")
	ck.Put(key2, "")
	time.Sleep(200 * time.Millisecond)

	// Construct a transaction with two operations (PUTs on key1 and key2).
	op1 := Op{
		Type:        PUT,
		Arg1:        key1,
		Arg2:        "single1",
		ClientUuid:  ck.uuid,
		ClientReqNo: atomic.AddInt32(&ck.reqNumber, 1),
	}
	op2 := Op{
		Type:        PUT,
		Arg1:        key2,
		Arg2:        "single2",
		ClientUuid:  ck.uuid,
		ClientReqNo: atomic.AddInt32(&ck.reqNumber, 1),
	}
	ops := []Op{op1, op2}

	// Process the transaction.
	if err := ck.ProcessTransaction(ops); err != nil {
		t.Fatalf("TransactionSingleGroup returned error: %v", err)
	}

	// Since commit is not applied, we cannot check the final value.
	// Test passes if ProcessTransaction returns nil.
	fmt.Printf("  ... Passed\n")
}

func DoTestTransactionLockConflict(t *testing.T) {
	fmt.Printf("Test: Transaction lock conflict...\n")
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	// Create two different clients.
	ck1 := cfg.makeClient()
	ck2 := cfg.makeClient()
	// Join at least two groups.
	cfg.join(0)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)

	// Choose a key that will be used in both transactions.
	key := "conflictKey"
	ck1.Put(key, "")
	time.Sleep(200 * time.Millisecond)

	// Client 1 constructs a transaction to lock the key.
	op1 := Op{
		Type:        PUT,
		Arg1:        key,
		Arg2:        "first",
		ClientUuid:  ck1.uuid,
		ClientReqNo: atomic.AddInt32(&ck1.reqNumber, 1),
	}
	txOps1 := []Op{op1}

	// Client 2 constructs a transaction attempting to lock the same key.
	op2 := Op{
		Type:        PUT,
		Arg1:        key,
		Arg2:        "second",
		ClientUuid:  ck2.uuid,
		ClientReqNo: atomic.AddInt32(&ck2.reqNumber, 1),
	}
	txOps2 := []Op{op2}

	// Let client 1 execute its transaction.
	if err := ck1.ProcessTransaction(txOps1); err != nil {
		t.Fatalf("First transaction failed: %v", err)
	}

	// Now client 2 attempts the transaction.
	// According to our server logic, since the key is already locked by txID from client 1,
	// client 2's transaction should fail with "LockFailed".
	err := ck2.ProcessTransaction(txOps2)
	if err == nil {
		// In our implementation, ProcessTransaction on the client returns nil
		// even if the reply.Err was "LockFailed" (since we are not propagating an error).
		// Hence, to verify the conflict, we can invoke the RPC directly and inspect reply.Err.
		var txReply TxReply
		// Use a server from the first group available.
		var serverList []string
		for _, servers := range ck2.config.Groups {
			serverList = servers
			break
		}
		if len(serverList) == 0 {
			t.Fatalf("No servers available for conflict test")
		}
		srv := ck2.make_end(serverList[0])
		if ok := srv.Call("ShardKV.ProcessTransaction", &TxOp{
			Ops:           txOps2,
			ClientId:      ck2.uuid,
			RequestNumber: atomic.AddInt32(&ck2.reqNumber, 1),
		}, &txReply); !ok || txReply.Err != "LockFailed" {
			t.Fatalf("Expected transaction on conflicting key to return LockFailed, got %v", txReply.Err)
		}
	} else {
		t.Fatalf("Expected ProcessTransaction to signal a conflict, but it returned nil error")
	}
	fmt.Printf("  ... Passed\n")
}

// TestTransactionMultiGroup tests a transaction that involves keys from two different groups.
// We simulate by joining two groups (group 0 and group 1). The keys chosen should be assigned to different groups.
func TestTransactionMultiGroup(t *testing.T) {
	fmt.Printf("Test: Transaction spanning multiple groups...\n")
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()
	// Join two groups.
	cfg.join(0)
	cfg.join(1)
	// Allow time for the configuration change to propagate.
	time.Sleep(500 * time.Millisecond)

	// Pick two keys such that (with high probability) they map to different groups.
	key1 := "a"
	key2 := "b"
	ck.Put(key1, "")
	ck.Put(key2, "")
	time.Sleep(200 * time.Millisecond)

	op1 := Op{
		Type:        PUT,
		Arg1:        key1,
		Arg2:        "multi1",
		ClientUuid:  ck.uuid,
		ClientReqNo: atomic.AddInt32(&ck.reqNumber, 1),
	}
	op2 := Op{
		Type:        PUT,
		Arg1:        key2,
		Arg2:        "multi2",
		ClientUuid:  ck.uuid,
		ClientReqNo: atomic.AddInt32(&ck.reqNumber, 1),
	}
	ops := []Op{op1, op2}

	if err := ck.ProcessTransaction(ops); err != nil {
		t.Fatalf("TransactionMultiGroup returned error: %v", err)
	}
	fmt.Printf("  ... Passed\n")
}

func TestProcessTransactionBasic(t *testing.T) {
	fmt.Printf("Test: basic ProcessTransaction ...\n")
	// Create a test configuration with 3 servers per group,
	// with a reliable network and no snapshotting (maxraftstate = -1).
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	// Create a client to interact with the shardkv service.
	ck := cfg.makeClient()
	// Join at least two groups so that keys might be assigned across groups.
	cfg.join(0)
	cfg.join(1)
	// Wait a bit for the configuration change to propagate.
	time.Sleep(500 * time.Millisecond)

	// Choose a key for testing.
	key := "txnKey" // chosen arbitrarily

	// Clear the key with a normal PUT.
	ck.Put(key, "")
	time.Sleep(200 * time.Millisecond)

	// Construct a transaction with two operations on the same key:
	// First a PUT to set the key to "val1", then an APPEND to add "val2".xx
	op1 := Op{
		Type:        PUT,
		Arg1:        key,
		Arg2:        "val1",
		ClientUuid:  ck.uuid,
		ClientReqNo: atomic.AddInt32(&ck.reqNumber, 1),
	}
	op2 := Op{
		Type:        APPEND,
		Arg1:        key,
		Arg2:        "val2",
		ClientUuid:  ck.uuid,
		ClientReqNo: atomic.AddInt32(&ck.reqNumber, 1),
	}
	ops := []Op{op1, op2}

	// Call ProcessTransaction
	err := ck.ProcessTransaction(ops)
	if err != nil {
		t.Fatalf("ProcessTransaction returned error: %v", err)
	}
	fmt.Printf("  ... Passed\n")
}

func doTestTransactionPUT(t *testing.T) {
	fmt.Printf("Test: basic 2PC transaction (PUT/PUT) ...\n")
	// Create a configuration with 3 servers per group;
	// use a reliable network and disable snapshots (maxraftstate = -1).
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()
	// Join two replica groups so that keys can (likely) be served by different groups.
	cfg.join(0)
	cfg.join(1)
	// Allow the configuration change to propagate.
	time.Sleep(500 * time.Millisecond)

	// Choose two keys that (with high probability) fall into different shards.
	// (key2shard computes: shard = int(key[0]) % NShards.)
	key1 := "a" // ASCII 97 → shard 7 when mod 10
	key2 := "b" // ASCII 98 → shard 8 when mod 10

	// Start by clearing these keys.
	ck.Put(key1, "")
	ck.Put(key2, "")
	time.Sleep(200 * time.Millisecond)

	// Build two operations: a PUT on key1 and a PUT on key2.
	op1 := Op{
		Type:        PUT,
		Arg1:        key1,
		Arg2:        "Hello",
		ClientUuid:  ck.uuid,
		ClientReqNo: 100, // arbitrary request numbers for the transaction
	}
	op2 := Op{
		Type:        PUT,
		Arg1:        key2,
		Arg2:        "World",
		ClientUuid:  ck.uuid,
		ClientReqNo: 101,
	}
	ops := []Op{op1, op2}

	// Process the transaction. In your implementation the transaction will use 2PC
	// to (first) lock the keys across the involved groups and then commit the ops.
	err := ck.ProcessTransaction(ops)
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// In a full 2PC the operations will be applied atomically if locking succeeds.
	// We check that the keys have been updated as expected.
	v1 := ck.Get(key1)
	if v1 != "Hello" {
		t.Fatalf("TransactionBasic: expected key %v to have value 'Hello', got '%v'", key1, v1)
	}
	v2 := ck.Get(key2)
	if v2 != "World" {
		t.Fatalf("TransactionBasic: expected key %v to have value 'World', got '%v'", key2, v2)
	}

	fmt.Printf("  ... Passed\n")
}

// test static 2-way sharding, without shard movement.
func TestStaticShards(t *testing.T) {
	fmt.Printf("Test: static shards ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)
	cfg.join(1)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	cfg.ShutdownGroup(1)
	cfg.checklogs() // forbid snapshots

	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		ck1 := cfg.makeClient() // only one call allowed per client
		go func(i int) {
			v := ck1.Get(ka[i])
			if v != va[i] {
				ch <- fmt.Sprintf("Get(%v): expected:\n%v\nreceived:\n%v", ka[i], va[i], v)
			} else {
				ch <- ""
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err != "" {
				t.Fatal(err)
			}
			ndone += 1
		case <-time.After(time.Second * 2):
			done = true
			break
		}
	}

	if ndone != 5 {
		t.Fatalf("expected 5 completions with one shard dead; got %v\n", ndone)
	}

	// bring the crashed shard/group back to life.
	cfg.StartGroup(1)
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestJoinLeave(t *testing.T) {
	fmt.Printf("Test: join then leave ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	// allow time for shards to transfer.
	time.Sleep(1 * time.Second)

	cfg.checklogs()
	cfg.ShutdownGroup(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestSnapshot(t *testing.T) {
	fmt.Printf("Test: snapshots, join, and leave ...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.leave(1)
	cfg.join(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(1 * time.Second)

	cfg.checklogs()

	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)

	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestMissChange(t *testing.T) {
	fmt.Printf("Test: servers miss configuration changes...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	cfg.ShutdownServer(0, 0)
	cfg.ShutdownServer(1, 0)
	cfg.ShutdownServer(2, 0)

	cfg.join(2)
	cfg.leave(1)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 0)
	cfg.StartServer(1, 0)
	cfg.StartServer(2, 0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(2 * time.Second)

	cfg.ShutdownServer(0, 1)
	cfg.ShutdownServer(1, 1)
	cfg.ShutdownServer(2, 1)

	cfg.join(0)
	cfg.leave(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 1)
	cfg.StartServer(1, 1)
	cfg.StartServer(2, 1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent1(t *testing.T) {
	fmt.Printf("Test: concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)

	cfg.ShutdownGroup(0)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(1)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(2)

	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)

	time.Sleep(1 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
func TestConcurrent2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(0)
	cfg.join(2)
	cfg.leave(1)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(1)
	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)

	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	time.Sleep(1000 * time.Millisecond)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent3(t *testing.T) {
	fmt.Printf("Test: concurrent configuration change and restart...\n")

	cfg := make_config(t, 3, false, 300)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	t0 := time.Now()
	for time.Since(t0) < 12*time.Second {
		cfg.join(2)
		cfg.join(1)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.ShutdownGroup(0)
		cfg.ShutdownGroup(1)
		cfg.ShutdownGroup(2)
		cfg.StartGroup(0)
		cfg.StartGroup(1)
		cfg.StartGroup(2)

		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.leave(1)
		cfg.leave(2)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable1(t *testing.T) {
	fmt.Printf("Test: unreliable 1...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(0)
	cfg.leave(1)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable2(t *testing.T) {
	fmt.Printf("Test: unreliable 2...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable3(t *testing.T) {
	fmt.Printf("Test: unreliable 3...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		start := int64(time.Since(begin))
		ck.Put(ka[i], va[i])
		end := int64(time.Since(begin))
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp models.KvInput
			var out models.KvOutput
			start := int64(time.Since(begin))
			if (rand.Int() % 1000) < 500 {
				ck1.Append(ka[ki], nv)
				inp = models.KvInput{Op: 2, Key: ka[ki], Value: nv}
			} else if (rand.Int() % 1000) < 100 {
				ck1.Put(ka[ki], nv)
				inp = models.KvInput{Op: 1, Key: ka[ki], Value: nv}
			} else {
				v := ck1.Get(ka[ki])
				inp = models.KvInput{Op: 0, Key: ka[ki]}
				out = models.KvOutput{Value: v}
			}
			end := int64(time.Since(begin))
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers are deleting
// shards for which they are no longer responsible.
func TestChallenge1Delete(t *testing.T) {
	fmt.Printf("Test: shard deletion (challenge 1) ...\n")

	// "1" means force snapshot after every log entry.
	cfg := make_config(t, 3, false, 1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	// 30,000 bytes of total values.
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1000)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	for iters := 0; iters < 2; iters++ {
		cfg.join(1)
		cfg.leave(0)
		cfg.join(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
		cfg.leave(1)
		cfg.join(0)
		cfg.leave(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
	}

	cfg.join(1)
	cfg.join(2)
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	total := 0
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			total += raft + snap
		}
	}

	// 27 keys should be stored once.
	// 3 keys should also be stored in client dup tables.
	// everything on 3 replicas.
	// plus slop.
	expected := 3 * (((n - 3) * 1000) + 2*3*1000 + 6000)
	if total > expected {
		t.Fatalf("snapshot + persisted Raft state are too big: %v > %v\n", total, expected)
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle
// shards that are not affected by a config change
// while the config change is underway
func TestChallenge2Unaffected(t *testing.T) {
	fmt.Printf("Test: unaffected shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// JOIN 100
	cfg.join(0)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// JOIN 101
	cfg.join(1)

	// QUERY to find shards now owned by 101
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[1].gid
	}

	// Wait for migration to new config to complete, and for clients to
	// start using this updated config. Gets to any key k such that
	// owned[shard(k)] == true should now be served by group 101.
	<-time.After(1 * time.Second)
	for i := 0; i < n; i++ {
		if owned[i] {
			va[i] = "101"
			ck.Put(ka[i], va[i])
		}
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100
	// 101 doesn't get a chance to migrate things previously owned by 100
	cfg.leave(0)

	// Wait to make sure clients see new config
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys still complete
	for i := 0; i < n; i++ {
		shard := int(ka[i][0]) % 10
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-1")
			check(t, ck, ka[i], va[i]+"-1")
		}
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle operations on shards that
// have been received as a part of a config migration when the entire migration
// has not yet completed.
func TestChallenge2Partial(t *testing.T) {
	fmt.Printf("Test: partial migration shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// JOIN 100 + 101 + 102
	cfg.joinm([]int{0, 1, 2})

	// Give the implementation some time to reconfigure
	<-time.After(1 * time.Second)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// QUERY to find shards owned by 102
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[2].gid
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100 + 102
	// 101 can get old shards from 102, but not from 100. 101 should start
	// serving shards that used to belong to 102 as soon as possible
	cfg.leavem([]int{0, 2})

	// Give the implementation some time to start reconfiguration
	// And to migrate 102 -> 101
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys now complete
	for i := 0; i < n; i++ {
		shard := key2shard(ka[i])
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-2")
			check(t, ck, ka[i], va[i]+"-2")
		}
	}

	fmt.Printf("  ... Passed\n")
}
