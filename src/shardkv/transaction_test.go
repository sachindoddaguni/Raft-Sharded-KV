package shardkv

import (
	"fmt"
	"testing"
	"time"
)

func TestTransactions(t *testing.T) {
	deleteContainersWithPrefix()
	fmt.Printf("Test: transactions ...\n")
	ng := 2
	cfg := make_config(t, 3, false, -1, ng)
	for g := range ng {
		cfg.join(g)
	}
	time.Sleep(5 * time.Second)
	ck := cfg.makeClient()
	op1 := Op{
		Type:        PUT,
		Arg1:        "xuv",
		Arg2:        "testing1",
		ClientUuid:  1,
		ClientReqNo: 1,
	}
	op2 := Op{
		Type:        PUT,
		Arg1:        "yam",
		Arg2:        "testing2",
		ClientUuid:  1,
		ClientReqNo: 1,
	}
	op3 := Op{
		Type:        PUT,
		Arg1:        "abc",
		Arg2:        "testing3",
		ClientUuid:  1,
		ClientReqNo: 1,
	}
	ck.ProcessTransaction([]Op{op1, op2, op3})
	time.Sleep(5 * time.Second)
	v := ck.Get("xuv")
	if v != "testing1" {
		t.Fatalf("expected x == \"100\" after transaction, got %q", v)
	}
	v = ck.Get("yam")
	if v != "testing2" {
		t.Fatalf("expected x == \"100\" after transaction, got %q", v)
	}
	v = ck.Get("abc")
	if v != "testing3" {
		t.Fatalf("expected x == \"100\" after transaction, got %q", v)
	}
}

func TestTransactionLockConflict(t *testing.T) {
	deleteContainersWithPrefix()
	fmt.Printf("Test: transaction lock conflict …\n")
	ng := 2
	cfg := make_config(t, 3, false, -1, ng)
	for g := range ng {
		cfg.join(g)
	}
	time.Sleep(5 * time.Second)
	ck1 := cfg.makeClient()
	ck2 := cfg.makeClient()
	go func() {
		tx := []Op{{Type: PUT, Arg1: "x", Arg2: "100", ClientUuid: 1, ClientReqNo: 1}}
		ck1.ProcessTransaction(tx, 1)
	}()
	// let ck1 grab the X lock first
	time.Sleep(50 * time.Millisecond)

	tx2 := []Op{{Type: PUT, Arg1: "x", Arg2: "200", ClientUuid: 2, ClientReqNo: 1}}
	err := ck2.ProcessTransaction(tx2)
	if err != nil && err.Error() != "LockFailed" {
		t.Fatalf("expected second transaction to fail with LockFailed, got %v", err)
	}
	fmt.Println("  … saw expected LockFailed")

	time.Sleep(10 * time.Second)

	v := ck1.Get("x")
	if v != "100" {
		t.Fatalf("expected x == \"100\" after transaction, got %q", v)
	}
}

func TestFaultTolerance(t *testing.T) {
	deleteContainersWithPrefix()
	fmt.Printf("Test: transaction fault tolerance …\n")
	ng := 1
	cfg := make_config(t, 3, false, -1, ng)
	for g := range ng {
		cfg.join(g)
	}
	time.Sleep(5 * time.Second)
	ck1 := cfg.makeClient()
	ck2 := cfg.makeClient()
	go func() {
		tx := []Op{{Type: PUT, Arg1: "x", Arg2: "100", ClientUuid: 1, ClientReqNo: 1}, {Type: PUT, Arg1: "y", Arg2: "100", ClientUuid: 1, ClientReqNo: 102}}
		ck1.ProcessTransaction(tx, 30)
	}()
	// let ck1 grab the X lock first
	time.Sleep(50 * time.Millisecond)
	for s := range 3 {
		_, leader := cfg.groups[0].servers[s].rf.GetState()
		if leader {
			fmt.Printf("leader is %d. Killing leader now \n", s)
			cfg.groups[0].servers[s].Kill()
			break
		}
	}
	time.Sleep(5 * time.Second)
	tx2 := []Op{{Type: PUT, Arg1: "x", Arg2: "200", ClientUuid: 2, ClientReqNo: 1}, {Type: PUT, Arg1: "y", Arg2: "200", ClientUuid: 2, ClientReqNo: 102}}
	err := ck2.ProcessTransaction(tx2)
	if err != nil && err.Error() == "LockFailed" {
		t.Fatalf("expected second transaction to fail with LockFailed, got %v", err)
	}
	time.Sleep(5 * time.Second)
	v := ck1.Get("x")
	if v != "200" {
		t.Fatalf("expected x == \"200\" after transaction, got %q", v)
	}
	v = ck1.Get("y")
	if v != "200" {
		t.Fatalf("expected y == \"200\" after transaction, got %q", v)
	}
}

func TestCrashRecovery(t *testing.T) {
	deleteContainersWithPrefix()
	fmt.Printf("Test: transactions ...\n")
	ng := 1
	cfg := make_config(t, 3, false, -1, ng)
	for g := range ng {
		cfg.join(g)
	}
	time.Sleep(5 * time.Second)
	ck := cfg.makeClient()
	op1 := Op{
		Type:        PUT,
		Arg1:        "x",
		Arg2:        "2",
		ClientUuid:  1,
		ClientReqNo: 1,
	}
	op2 := Op{
		Type:        PUT,
		Arg1:        "y",
		Arg2:        "3",
		ClientUuid:  1,
		ClientReqNo: 1,
	}
	op3 := Op{
		Type:        PUT,
		Arg1:        "a",
		Arg2:        "3",
		ClientUuid:  1,
		ClientReqNo: 1,
	}
	ck.ProcessTransaction([]Op{op1, op2, op3})
	time.Sleep(5 * time.Second)

	cfg.ShutdownServer(0, 0)

	time.Sleep(5 * time.Second)

	cfg.StartServer(0, 0)

	time.Sleep(5 * time.Second)
}
