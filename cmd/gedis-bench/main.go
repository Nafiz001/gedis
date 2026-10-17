// Command gedis-bench measures throughput and latency against a gedis or Redis
// server. It speaks plain RESP, so it can point at either and the comparison is
// apples to apples.
//
//	gedis-bench -addr 127.0.0.1:6380 -c 50 -n 100000 -t set,get,incr
//	gedis-bench -addr 127.0.0.1:6379 -c 50 -n 100000 -P 16
//
// Latency is reported as percentiles rather than a mean. A mean hides the tail,
// and the tail is what users actually notice.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Nafiz001/gedis/internal/resp"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:6380", "server address")
		clients  = flag.Int("c", 50, "concurrent connections")
		requests = flag.Int("n", 100000, "total requests per test")
		pipeline = flag.Int("P", 1, "commands to pipeline per round trip")
		tests    = flag.String("t", "ping,set,get,incr", "comma-separated tests")
		keyspace = flag.Int("r", 10000, "number of distinct keys to spread writes over")
		valueLen = flag.Int("d", 16, "value size in bytes")
	)
	flag.Parse()

	if *pipeline < 1 {
		*pipeline = 1
	}

	fmt.Printf("gedis-bench -> %s\n", *addr)
	fmt.Printf("%d requests, %d clients, pipeline %d, %d-byte values, %d keys\n\n",
		*requests, *clients, *pipeline, *valueLen, *keyspace)

	value := strings.Repeat("x", *valueLen)

	fmt.Printf("%-8s %12s %10s %10s %10s %10s\n",
		"TEST", "REQ/SEC", "AVG ms", "P50 ms", "P99 ms", "MAX ms")
	fmt.Println(strings.Repeat("-", 66))

	for _, name := range strings.Split(*tests, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		res, err := runTest(name, *addr, *clients, *requests, *pipeline, *keyspace, value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
		res.print(name)
	}
}

type result struct {
	elapsed  time.Duration
	requests int
	// latencies holds one sample per round trip, in microseconds.
	latencies []int64
}

func (r result) print(name string) {
	rps := float64(r.requests) / r.elapsed.Seconds()
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })

	pct := func(p float64) float64 {
		if len(r.latencies) == 0 {
			return 0
		}
		i := int(p * float64(len(r.latencies)-1))
		return float64(r.latencies[i]) / 1000
	}
	var sum int64
	for _, l := range r.latencies {
		sum += l
	}
	avg := 0.0
	if len(r.latencies) > 0 {
		avg = float64(sum) / float64(len(r.latencies)) / 1000
	}

	fmt.Printf("%-8s %12.0f %10.3f %10.3f %10.3f %10.3f\n",
		strings.ToUpper(name), rps, avg, pct(0.50), pct(0.99), pct(1.0))
}

// buildCommand returns the arguments for one request of the named test.
func buildCommand(name string, rng *rand.Rand, keyspace int, value string) ([]string, error) {
	key := fmt.Sprintf("bench:key:%d", rng.Intn(keyspace))
	switch name {
	case "ping":
		return []string{"PING"}, nil
	case "set":
		return []string{"SET", key, value}, nil
	case "get":
		return []string{"GET", key}, nil
	case "incr":
		return []string{"INCR", "bench:counter:" + fmt.Sprint(rng.Intn(keyspace))}, nil
	case "del":
		return []string{"DEL", key}, nil
	case "exists":
		return []string{"EXISTS", key}, nil
	case "setex":
		return []string{"SET", key, value, "EX", "60"}, nil
	default:
		return nil, fmt.Errorf("unknown test %q", name)
	}
}

func runTest(name, addr string, clients, requests, pipeline, keyspace int, value string) (result, error) {
	// Validate the test name once, before opening any connections.
	if _, err := buildCommand(name, rand.New(rand.NewSource(1)), keyspace, value); err != nil {
		return result{}, err
	}

	perClient := requests / clients
	if perClient < 1 {
		perClient = 1
	}
	rounds := perClient / pipeline
	if rounds < 1 {
		rounds = 1
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		samples []int64
		failed  error
	)

	start := time.Now()
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				mu.Lock()
				if failed == nil {
					failed = err
				}
				mu.Unlock()
				return
			}
			defer conn.Close()

			r := resp.NewReaderSize(conn, 64*1024)
			w := resp.NewWriterSize(conn, 64*1024)
			rng := rand.New(rand.NewSource(int64(seed)))
			local := make([]int64, 0, rounds)

			for round := 0; round < rounds; round++ {
				t0 := time.Now()
				for p := 0; p < pipeline; p++ {
					args, _ := buildCommand(name, rng, keyspace, value)
					if err := w.WriteCommand(args...); err != nil {
						mu.Lock()
						if failed == nil {
							failed = err
						}
						mu.Unlock()
						return
					}
				}
				if err := w.Flush(); err != nil {
					mu.Lock()
					if failed == nil {
						failed = err
					}
					mu.Unlock()
					return
				}
				for p := 0; p < pipeline; p++ {
					if _, err := r.ReadValue(); err != nil {
						mu.Lock()
						if failed == nil {
							failed = err
						}
						mu.Unlock()
						return
					}
				}
				// One sample per round trip: with pipelining this is the
				// latency of the whole batch, which is what the client waits on.
				local = append(local, time.Since(t0).Microseconds())
			}

			mu.Lock()
			samples = append(samples, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if failed != nil {
		return result{}, failed
	}
	return result{
		elapsed:   elapsed,
		requests:  clients * rounds * pipeline,
		latencies: samples,
	}, nil
}
