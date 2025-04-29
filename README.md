# Fault-tolerant Sharded Key-Value Storage service

<img width="1012" alt="image" src="https://github.com/user-attachments/assets/78041db7-edaf-4973-bcda-a24e4b6baa5e" />

Existing Features

- Raft Layer
- Fault Tolerant : Replication
- Dynamic Sharding

Features Added:

- Fault tolerant 2PC coordinator
- Spanner Like 2 PC
- Network I/O for all the layers
- Containerized implementation for all the layers


## Steps to run

### Requirements
- 16GB+ RAM
- Tested on MAC M3, 16GB RAM
- Docker
- Go : 1.22.3^

### Running Containers
The setup does not have  CLI. Since interface are thorugh code(SDK), the following tests helps to visualise the containers and networl communications.
 [`transaction_test.go`](./src/shardkv/transaction_test.go) : contains 4 tests where each tests spwans up new environment with the configured number of containers.
  - Test 1: Test 1 spwans up 2 replica groups i.e 6 containers. 3 containers are spawned up for shard controllers. The test further tests a output of a simple transaction with 3 put operations
  - Test 2: Test 2 spawns an environment similar to test 1. 9 total containers are created for each machine. The test specifically tests a conflict scenario where two transactions operate on the same data. We expect the system to abort one of the transactions.
  - Test 3: The test 3 aims at testing the fault tolerance feature of the coordinator. The tests kills a coordinator holding locks and tests whether the system recovers from this scenario. The test expects the new leader to clear all the pre held locks when a new leader is elected.
  - Test 4: The test 4 tests the crash recovery ability of an individual machine. Each machine recovers its state from the RAFT logs and the tests ensures that the system comes into a valid state after crash recovery.
 
## Project Contribution:
### Sachin
- [`transaction.go`](./src/shardkv/transaction.go) - Developed the client SDK to communicate with the shard controllers and replica groups
  - _ProcessTransaction_ - SDK call which interfaces with the shard controller to fetch all the replica groups and assigns one of them as the coordinator
  - _Get_ - Simple Get interface
  - _Put_ - Simple Put interface
  - _PutAppend_ - Append to an existing value 
- [`server.go`](./src/shardkv/server.go) - Contains the code for various server side RPCs
  - _ProcessTransaction_ - RPC to process a trnasaction as a transaction coordinator
  - _LockKeys_ - RPC to Lock Keys which are mentioned in the arguments
  - _UnLockKeys_ - RPC to Unlock specified keys
  - _abortAllPending_ - Abort all pending transactions. The method is used during crash recovery.
  - _applyHandler_ - Handle specific RPCs
  - _Kill_ - Kill a specific server i.e container in our setup
  - _stateCompactor_ - Used for snaphotting RAFT logs. RAFT logs are compacted for every 20 entries and the in memory state is stored. 
- [`common.go`](./src/shardkv/common.go) - Contains the code for common unitilit behaviour
  - _createContainer_ - Create Container for each machine. Stores the container id and public port in a struct
  - _deleteContainersWithPrefix_ - Used to clean up environment
  - _logToServer_ - common logging function
- [`serverctler.go`](./src/shardctrler/server.go)
  - _StartServer_ - Start a shard controller container service
  - _handleLeave_ - Handle replica group machine movement i.e leave or join
- [`raft.go`](./src/raft/raft.go)
  - _persist_ - persist raft entries to on disk logs i.e to a log file on the container disk
  - _snapshot_ - compact raft entries and store the snapshot in disk
  - _readPersist_ - Read in memory state from raft logs
- [`Dockerfile`](./src/docker-setup/Dockerfile) - Docker Image Setup
  - Each docker image has both the RAFT layer code and Application layer code. The dockerfile packages both the layers into a single image.   

---
### Samarth

- [`encoder.go`](./src/rpc/kvrpc/encoder.go)  
  Wraps `labgob.LabEncoder` into a simple `Encode(v interface{})` API that all RPCs use to serialize request and response objects before writing them to the wire.

- [`decoder.go`](./src/rpc/kvrpc/decoder.go)  
  Wraps `labgob.LabDecoder` into a simple `Decode(v interface{})` API that all RPC handlers and clients use to deserialize incoming bytes back into Go structs.

- [`codec.go`](./src/rpc/kvrpc/codec.go)  
  Implements both `rpc.ServerCodec` and `rpc.ClientCodec` over any `io.ReadWriteCloser`:  
  - `NewServerCodec(conn)` → used in `rpc.ServeCodec` on the server side  
  - `NewClientCodec(conn)` → used in `rpc.NewClientWithCodec` on the client side  
  It wires together our `Encoder`/`Decoder` with request/response header marshaling, ensuring thread-safe writes.

- [`register.go`](./src/rpc/kvrpc/register.go)  
  Contains `init()`-time calls to `labgob.Register(...)` for every RPC argument and reply type (`Op`, `ConfigChange`, `LockArgs`, etc.), so you never forget to register a new struct and encounter decode failures.

- [`marshal.go`](./src/rpc/kvrpc/marshal.go)  
  Provides standalone `Marshal(v)` / `Unmarshal(data, &v)` helpers based on `LabEncoder`/`LabDecoder`, used for snapshotting or any place you need to turn structs into `[]byte` and back.

- [`listener.go`](./src/rpc/kvrpc/listener.go)  
  A helper function that:  
  1. `rpc.RegisterName(name, svc)`  
  2. `net.Listen("tcp", addr)`  
  3. Accepts connections in a loop and calls `rpc.ServeCodec(NewServerCodec(conn))`

- [`bufferpool.go`](./src/rpc/kvrpc/bufferpool.go)  
  Defines a `sync.Pool` of `bytes.Buffer` to reuse buffers for marshalling, reducing GC pressure under high-rate RPC workloads.

- [`dialer.go`](./src/rpc/dialer.go)  
  Exports `DialWithTimeout(addr, timeout)` which:  
  1. `net.DialTimeout("tcp", addr, timeout)`  
  2. Wraps the returned `net.Conn` in `labgobrpc.NewClientCodec`  
  3. Returns an `*rpc.Client`  
  This centralizes all host:port parsing, timeouts, and codec wiring for clients.

- [`benchmark.go`](./src/performance/benchmark.go)  
  Our Go benchmarking driver, configurable via flags (`-scenario`, `-clients`, `-duration`, etc.).  
  - **Initialization**: parses flags, constructs a shard‐controller clerk, issues initial `Join`.  
  - **Client Workers**: spawns goroutines issuing `Get`, `Get+lock`, or various `ProcessTransaction` patterns, recording per-op latencies.  
  - **Aggregation & Statistics**: computes mean, p95, p99 latencies and throughput.  
  - **Workload Generators**: functions for uniform vs. hot-key GETs, conflict‐ratio transactions, single‐group vs. cross‐group batches, and fan‐out patterns.
