// cmd/bench/main.go
package performance

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"6.824/shardkv"
)

const (
	TotalKeys      = 10000
	NShards        = 9
	ShardsPerGroup = 3 // for 3 replica-groups
)

var (
	ctrlAddrs     = flag.String("ctrl", "localhost:1234,localhost:1235,localhost:1236", "comma-separated shard controller addresses")
	scenario      = flag.String("scenario", "ro", "workload: ro | rolock | txconflict | txsingle | txcross | txsize")
	clients       = flag.Int("c", 50, "number of concurrent clients")
	duration      = flag.Duration("d", 60*time.Second, "benchmark duration")
	conflictRatio = flag.Float64("conflict", 0.0, "conflict ratio for txconflict (0.0-1.0)")
	txSize        = flag.Int("txsize", 3, "transaction size for txsize/single/cross scenarios")
	pattern       = flag.String("pattern", "colocated", "fan-out pattern for txsize: colocated | spread")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// Shard controller clerk for initial join
	ctrlEnds := make([]*shardkv.ClerkEnd, 0)
	for _, addr := range strings.Split(*ctrlAddrs, ",") {
		ctrlEnds = append(ctrlEnds, shardkv.NewClerkEnd(addr))
	}
	sc := shardkv.MakeClerk(ctrlEnds, makeEnd)
	// assume groups 100,101,102 join at start
	sc.Join(map[int][]string{
		100: {"kv-100-0:2379", "kv-100-1:2379", "kv-100-2:2379"},
		101: {"kv-101-0:2379", "kv-101-1:2379", "kv-101-2:2379"},
		102: {"kv-102-0:2379", "kv-102-1:2379", "kv-102-2:2379"},
	})

	var wg sync.WaitGroup
	stopCh := time.After(*duration)
	results := make(chan time.Duration, *clients*1000)

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// each client has its own clerk
			ck := shardkv.MakeClerk(ctrlEnds, makeEnd)
			for {
				select {
				case <-stopCh:
					return
				default:
					start := time.Now()
					switch *scenario {
					case "ro":
						ck.Get(randomKey())
					case "rolock":
						ops := []shardkv.Op{{Type: shardkv.GET, Arg1: randomKey()}}
						ck.ProcessTransaction(ops)
					case "txconflict":
						ops := randomConflictTxOps(*conflictRatio)
						ck.ProcessTransaction(ops)
					case "txsingle":
						ops := randomSingleGroupTxOps(*txSize)
						ck.ProcessTransaction(ops)
					case "txcross":
						ops := randomCrossGroupTxOps(*txSize)
						ck.ProcessTransaction(ops)
					case "txsize":
						ops := randomFanoutTxOps(*txSize, *pattern)
						ck.ProcessTransaction(ops)
					default:
						// unknown scenario
					}
					results <- time.Since(start)
				}
			}
		}()
	}

	// collect latencies
	var latencies []time.Duration
	go func() {
		for d := range results {
			latencies = append(latencies, d)
		}
	}()

	wg.Wait()
	close(results)

	// compute stats and print
	mean, p95, p99 := computeStats(latencies)
	tputs := float64(len(latencies)) / duration.Seconds()
	fmt.Printf("Scenario=%s Clients=%d Duration=%v\n", *scenario, *clients, *duration)
	fmt.Printf("Throughput: %.2f tx/s\n", tputs)
	fmt.Printf("Latency: mean=%.2fms p95=%.2fms p99=%.2fms\n",
		mean.Seconds()*1000, p95.Seconds()*1000, p99.Seconds()*1000)
}

// makeEnd returns an RPC client end for a given server address.
func makeEnd(addr string) *shardkv.ClerkEnd {
	return shardkv.NewClerkEnd(addr)
}

// randomKey picks a uniform key.
func randomKey() string {
	return fmt.Sprintf("%05d", rand.Intn(TotalKeys))
}

// hotKey picks from the hot 10% of the keyspace.
func hotKey() string {
	hot := TotalKeys / 10
	return fmt.Sprintf("%05d", rand.Intn(hot))
}

// randomConflictTxOps generates a 3-key tx with given conflict ratio.
func randomConflictTxOps(conflict float64) []shardkv.Op {
	ops := make([]shardkv.Op, 3)
	for i := 0; i < 3; i++ {
		key := randomKey()
		if rand.Float64() < conflict {
			key = hotKey()
		}
		ops[i] = shardkv.Op{Type: shardkv.PUT, Arg1: key, Arg2: randString(5)}
	}
	return ops
}

// randomSingleGroupTxOps picks txSize keys from a single shard group.
func randomSingleGroupTxOps(size int) []shardkv.Op {
	// choose a group at random: 0,1,2
	group := rand.Intn(3)
	// each group owns shards [group*3, group*3+2]
	start := group * (TotalKeys / NShards) * ShardsPerGroup
	ops := make([]shardkv.Op, size)
	for i := 0; i < size; i++ {
		idx := start + rand.Intn(TotalKeys/3)
		key := fmt.Sprintf("%05d", idx)
		ops[i] = shardkv.Op{Type: shardkv.PUT, Arg1: key, Arg2: randString(5)}
	}
	return ops
}

// randomCrossGroupTxOps picks txSize keys evenly across groups.
func randomCrossGroupTxOps(size int) []shardkv.Op {
	ops := make([]shardkv.Op, size)
	for i := 0; i < size; i++ {
		group := i % 3
		start := group * (TotalKeys / NShards) * ShardsPerGroup
		idx := start + rand.Intn(TotalKeys/3)
		key := fmt.Sprintf("%05d", idx)
		ops[i] = shardkv.Op{Type: shardkv.PUT, Arg1: key, Arg2: randString(5)}
	}
	return ops
}

// randomFanoutTxOps supports colocated or spread patterns.
func randomFanoutTxOps(size int, pattern string) []shardkv.Op {
	if pattern == "colocated" {
		return randomSingleGroupTxOps(size)
	}
	return randomCrossGroupTxOps(size)
}

// randString generates a short random string.
func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// computeStats returns mean, p95, p99.
func computeStats(durs []time.Duration) (mean, p95, p99 time.Duration) {
	if len(durs) == 0 {
		return
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	var sum time.Duration
	for _, d := range durs {
		sum += d
	}
	mean = time.Duration(int64(sum) / int64(len(durs)))
	p95 = durs[int(float64(len(durs))*0.95)]
	p99 = durs[int(float64(len(durs))*0.99)]
	return
}
