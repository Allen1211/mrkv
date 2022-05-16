package test

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allen1211/mrkv/pkg/client"
	"github.com/allen1211/mrkv/pkg/common"
)

const (
	defaultTotal = 1 << 16
)

type PerformanceTest struct {
	masters		[]string
	threads 	int
	length		int
	total		int
	clients		[]*client.MrKVClient
}

func MakePerformanceTest(masters []string, threads, length, total int) *PerformanceTest {
	if total == -1 {
		total = defaultTotal
	}
	pt := &PerformanceTest{
		masters: masters,
		threads: threads,
		length:  length,
		total:   total,
		clients: make([]*client.MrKVClient, threads),
	}

	for i := 0; i < pt.threads; i++ {
		pt.clients[i] = client.MakeMrKVClient(pt.masters)
	}

	return pt
}

func (pt *PerformanceTest) Prepare()  {
	var total int
	if pt.total == -1 {
		total = defaultTotal
	} else {
		total = pt.total
	}
	val :=  pt.randstring(pt.length)
	fromKey, toKey := 0, total / pt.threads + (total % pt.threads)
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i, from, to int) {
			ck := pt.clients[i]
			for i := from; i < to; i++ {
				k, v := fmt.Sprintf("%d", i), val
				reply := ck.Put(k, []byte(v))
				if reply.Err != common.OK {
					fmt.Println(reply.Err)
				}
			}
			wg.Done()
		}(j, fromKey, toKey)
		fromKey = toKey
		toKey += total / pt.threads
	}
	wg.Wait()

}

func (pt *PerformanceTest) TestWriteOnly() {
	stat := MakePerformanceStat()
	val :=  pt.randstring(pt.length)

	fromKey, toKey := 0, pt.total / pt.threads + (pt.total % pt.threads)
	go stat.run()
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i, from, to int) {
			ck := pt.clients[i]
			for i := from; i < to; i++ {
				k, v := fmt.Sprintf("%d", i), val
				begin := time.Now()
				reply := ck.Put(k, []byte(v))
				cost := time.Since(begin).Nanoseconds()
				if reply.Err != common.OK {
					fmt.Println(reply.Err)
					stat.incrFail()
				} else {
					stat.incrSuccess(int64(pt.length), cost)
				}
			}
			wg.Done()
		}(j, fromKey, toKey)
		fromKey = toKey
		toKey += pt.total / pt.threads
	}
	wg.Wait()
	stat.stop()
}

func (pt *PerformanceTest) TestReadOnly() {
	stat := MakePerformanceStat()

	fromKey, toKey := 0, pt.total / pt.threads + (pt.total % pt.threads)

	go stat.run()
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i, from, to int) {
			ck := pt.clients[i]
			for i := from; i < to; i++ {
				k := fmt.Sprintf("%d", i)
				begin := time.Now()
				reply := ck.Get(k)
				cost := time.Since(begin).Nanoseconds()
				if reply.Err != common.OK && reply.Err != common.ErrNoKey {
					stat.incrFail()
					fmt.Println(reply.Err)
				} else {
					stat.incrSuccess(int64(pt.length), cost)
				}
			}
			wg.Done()
		}(j, fromKey, toKey)
		fromKey = toKey
		toKey += pt.total / pt.threads
	}
	wg.Wait()
	stat.stop()
}

func (pt *PerformanceTest) TestReadWrite() {
	stat := MakePerformanceStat()

	fromKey, toKey := 0, pt.total / pt.threads + (pt.total % pt.threads)
	val :=  pt.randstring(pt.length)

	go stat.run()
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i, from, to int) {
			rd := rand.New(rand.NewSource(time.Now().UnixNano()))
			ck := pt.clients[i]
			for i := from; i < to; i++ {
				k := fmt.Sprintf("%d", i)
				if rd.Intn(10) < 3 {
					begin := time.Now()
					reply := ck.Put(k, []byte(val))
					cost := time.Since(begin).Nanoseconds()
					if reply.Err != common.OK {
						fmt.Println(reply.Err)
						stat.incrFail()
					} else {
						stat.incrSuccess(int64(pt.length), cost)
					}
				} else {
					begin := time.Now()
					reply := ck.Get(k)
					cost := time.Since(begin).Nanoseconds()
					if reply.Err != common.OK && reply.Err != common.ErrNoKey {
						stat.incrFail()
						fmt.Println(reply.Err)
					} else {
						stat.incrSuccess(int64(pt.length), cost)
					}
				}
			}
			wg.Done()
		}(j, fromKey, toKey)
		fromKey = toKey
		toKey += pt.total / pt.threads
	}
	wg.Wait()
	stat.stop()
}

func (pt *PerformanceTest) randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type PerformanceStat struct {
	mu          sync.RWMutex
	success 	int64
	fail     	int64
	flow		int64
	totalSuccess 	int64
	totalFail     	int64
	totalFlow		int64

	lat 		int64
	minLat		int64
	maxLat		int64
	totalLat	int64

	begin		time.Time
	done		chan int
}

func MakePerformanceStat() PerformanceStat {
	return PerformanceStat{
		mu: 	sync.RWMutex{},
		done:   make(chan int),
	}
}

func (stat *PerformanceStat) incrSuccess(length, cost int64) {
	atomic.AddInt64(&stat.success, 1)
	atomic.AddInt64(&stat.totalSuccess, 1)
	atomic.AddInt64(&stat.flow, length)
	atomic.AddInt64(&stat.totalFlow, length)
	atomic.AddInt64(&stat.lat, cost)
	atomic.AddInt64(&stat.totalLat, cost)
}

func (stat *PerformanceStat) incrFail() {
	atomic.AddInt64(&stat.fail, 1)
	atomic.AddInt64(&stat.totalFail, 1)
}

func (stat *PerformanceStat) stop() {
	stat.done <- 1

	totalCost := time.Since(stat.begin).Seconds()
	qps := float64(stat.totalSuccess) / totalCost
	total := stat.totalSuccess + stat.totalFail
	success := float64(stat.totalSuccess) / float64(total) * 100
	throughput := float64(stat.totalFlow) / totalCost
	lat := (stat.totalLat/1000000) / stat.totalSuccess

	var throughputUnit string
	if throughput < (1 << 10) {
		throughputUnit = "B/s"
	} else if throughput < (1 << 20) {
		throughputUnit = "KB/s"
		throughput /= 1024
	} else if throughput < (1 << 30) {
		throughputUnit = "MB/s"
		throughput /= 1024 * 1024
	}

	fmt.Printf("total=%d, average qps=%.1f \t throughput=%.1f%s \t latency=%dms \t success=%.1f%%\n",
		total, qps, throughput, throughputUnit, lat, success)
}

func (stat *PerformanceStat) run() {
	stat.begin = time.Now()
	for {
		select {
		case <-stat.done:
			return

		case <-time.After(time.Second):

			success := atomic.SwapInt64(&stat.success, 0)
			fail := atomic.SwapInt64(&stat.fail, 0)
			throughput := atomic.SwapInt64(&stat.flow, 0)
			if success == 0 {
				success = 1
			}
			lat := (atomic.SwapInt64(&stat.lat, 0) / 1000000) / success

			var throughputUnit string
			if throughput < (1 << 10) {
				throughputUnit = "B/s"
			} else if throughput < (1 << 20) {
				throughputUnit = "KB/s"
				throughput /= 1024
			} else if throughput < (1 << 30) {
				throughputUnit = "MB/s"
				throughput /= 1024 * 1024
			}

			fmt.Printf("qps=%d \t throughput=%d%s \t latency=%dms \t success=%.2f%%\n",success, throughput, throughputUnit,
				lat, float64(success)/float64(success+fail)*100)

		}
	}
}

