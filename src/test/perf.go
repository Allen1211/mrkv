package test

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mrkv/src/client"
	"mrkv/src/common"
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

	per := total / pt.threads
	val :=  pt.randstring(pt.length)
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i int) {
			ck := pt.clients[i]
			begin := i * per
			for i := begin; i < begin + per; i++ {
				k, v := fmt.Sprintf("%d", i), val
				reply := ck.Put(k, []byte(v))
				if reply.Err != common.OK {
					fmt.Println(reply.Err)
				}
			}
			wg.Done()
		}(j)
	}
	wg.Wait()

}

func (pt *PerformanceTest) TestWriteOnly() {
	stat := MakePerformanceStat()
	per := pt.total / pt.threads
	val :=  pt.randstring(pt.length)

	go stat.run()
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i int) {
			ck := pt.clients[i]
			begin := i * per
			for i := begin; i < begin + per; i++ {
				k, v := fmt.Sprintf("%d", i), val
				reply := ck.Put(k, []byte(v))
				if reply.Err != common.OK {
					fmt.Println(reply.Err)
					stat.incrFail()
				} else {
					stat.incrSuccess(int64(pt.length))
				}
			}
			wg.Done()
		}(j)
	}
	wg.Wait()
	stat.stop()
}

func (pt *PerformanceTest) TestReadOnly() {
	stat := MakePerformanceStat()

	per := pt.total / pt.threads

	go stat.run()
	var wg sync.WaitGroup
	for j := 0; j < pt.threads; j++ {
		wg.Add(1)
		go func(i int) {
			ck := pt.clients[i]
			begin := i * per
			for i := begin; i < begin + per; i++ {
				k := fmt.Sprintf("%d", i)
				reply := ck.Get(k)
				if reply.Err != common.OK && reply.Err != common.ErrNoKey {
					stat.incrFail()
					fmt.Println(reply.Err)
				} else {
					stat.incrSuccess(int64(pt.length))
				}
			}
			wg.Done()
		}(j)
	}
	wg.Wait()
	stat.stop()
}

func (pt *PerformanceTest) TestReadWrite() {

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
	begin		time.Time
	done		chan int
}

func MakePerformanceStat() PerformanceStat {
	return PerformanceStat{
		mu: 	sync.RWMutex{},
		done:   make(chan int),
	}
}

func (stat *PerformanceStat) incrSuccess(length int64) {
	atomic.AddInt64(&stat.success, 1)
	atomic.AddInt64(&stat.flow, length)
}

func (stat *PerformanceStat) incrFail() {
	atomic.AddInt64(&stat.fail, 1)
}

func (stat *PerformanceStat) stop() {
	stat.done <- 1

	totalCost := time.Since(stat.begin).Seconds()
	qps := float64(stat.totalSuccess) / totalCost
	total := stat.totalSuccess + stat.totalFail
	success := float64(stat.totalSuccess) / float64(total) * 100
	throughput := float64(stat.totalFlow) / totalCost

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

	fmt.Printf("total=%d, average qps=%.1f \t throughput=%.1f%s \t success=%.1f%%\n",
		total, qps, throughput, throughputUnit, success)
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
			flow := atomic.SwapInt64(&stat.flow, 0)
			stat.totalSuccess += success
			stat.totalFail += fail
			stat.totalFlow += flow
			throughput := flow

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

			fmt.Printf("qps=%d \t throughput=%d%s \t success=%.2f%%\n",success, throughput, throughputUnit,
				float64(success)/float64(success+fail)*100)

		}
	}
}

