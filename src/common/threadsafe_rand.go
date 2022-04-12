package common

import (
	"math/rand"
	"sync"
)

type ThreadSafeRand struct {
	r  *rand.Rand
	mu sync.Mutex
}

func MakeThreadSafeRand(seed int64) ThreadSafeRand {
	r := rand.New(rand.NewSource(seed))
	return ThreadSafeRand{r: r, mu: sync.Mutex{}}
}

func (tsr *ThreadSafeRand) Intn(n int) int {
	tsr.mu.Lock()
	res := tsr.r.Intn(n)
	tsr.mu.Unlock()
	return res
}