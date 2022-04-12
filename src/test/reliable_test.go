package test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mrkv/src/client"
	"mrkv/src/common"
	"mrkv/src/netw"
	"mrkv/src/test/porcupine"
)

func crudWithLinearCheck(n int, gg func(), t *testing.T) bool {

	cli := client.MakeMrKVClient(masterAddrs)

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(5)

		start := int64(time.Since(begin).Nanoseconds())
		if reply := cli.Put(ka[i], []byte(va[i])); reply.Err != common.OK && reply.Err != common.ErrNoKey {
			t.Fatalf("operation failed: %s", reply.Err)
		}
		end := int64(time.Since(begin).Nanoseconds())

		inp := porcupine.KvInput{Op: porcupine.OpPut, Key: ka[i], Value: va[i]}
		var out porcupine.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		cli := client.MakeMrKVClient(masterAddrs)

		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp porcupine.KvInput
			var out porcupine.KvOutput
			start := int64(time.Since(begin).Nanoseconds())

			rd := rand.Int() % 1000
			if rd < 300 {
				if reply := cli.Append(ka[ki], []byte(nv)); reply.Err != common.OK && reply.Err != common.ErrNoKey {
					t.Fatalf("operation failed: %s", reply.Err)
				}
				inp = porcupine.KvInput{Op: porcupine.OpAppend, Key: ka[ki], Value: nv}
			} else if rd < 500 {
				if reply := cli.Put(ka[ki], []byte(nv)); reply.Err != common.OK && reply.Err != common.ErrNoKey {
					t.Fatalf("operation failed: %s", reply.Err)
				}
				inp = porcupine.KvInput{Op: porcupine.OpPut, Key: ka[ki], Value: nv}
			} else if rd < 600 {
				if reply := cli.Delete(ka[ki]); reply.Err != common.OK && reply.Err != common.ErrNoKey {
					t.Fatalf("operation failed: %s", reply.Err)
				}
				inp = porcupine.KvInput{Op: porcupine.OpDelete, Key: ka[ki]}
			} else {
				reply := cli.Get(ka[ki])
				if reply.Err != common.OK && reply.Err != common.ErrNoKey {
					t.Fatalf("operation failed: %s", reply.Err)
				}
				inp = porcupine.KvInput{Op: porcupine.OpGet, Key: ka[ki]}
				out = porcupine.KvOutput{Value: string(reply.Value)}
			}

			end := int64(time.Since(begin).Nanoseconds())
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	gg()
	atomic.StoreInt32(&done, 1)

	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(porcupine.KvModel, operations, 3 * time.Second)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(porcupine.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		return false
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	return true
}

func TestLinearizabilityNormal(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(8*time.Second)

	pass := crudWithLinearCheck(10, func() {
		time.Sleep(5 * time.Second)
	}, t)

	if !pass {
		t.Fatalf("Linearizability check fail")
	}

	stopNode(nodes...)
	stopMaster(masters...)
}

func TestLinearizabilityTimeout(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(8*time.Second)

	pass := crudWithLinearCheck(10, func() {
		time.Sleep(1 * time.Second)
		netw.SetUnreliable(5)
		time.Sleep(5 * time.Second)
		netw.SetUnreliable(0)
		time.Sleep(5 * time.Second)
		netw.SetUnreliable(10)
		time.Sleep(10 * time.Second)
		netw.SetUnreliable(0)
		time.Sleep(5 * time.Second)
	}, t)

	if !pass {
		t.Fatalf("Linearizability check fail")
	}

	stopNode(nodes...)
	stopMaster(masters...)
}


func TestLinearizabilityRestart(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(8*time.Second)

	pass := crudWithLinearCheck(10, func() {

		stopMaster(masters[0], masters[1])
		time.Sleep(1 * time.Second)
		masters[0] = startMaster(0)
		masters[1] = startMaster(1)
		time.Sleep(2 * time.Second)

		stopNode(nodes[0])
		time.Sleep(3 * time.Second)

		stopNode(nodes[1])
		time.Sleep(3 * time.Second)

		nodes[0] = startNode(1)
		time.Sleep(3 * time.Second)

		nodes[1] = startNode(2)
		time.Sleep(3 * time.Second)

		stopNode(nodes[2])
		time.Sleep(3 * time.Second)

		stopNode(nodes[3])
		time.Sleep(3 * time.Second)

		nodes[2] = startNode(3)
		time.Sleep(3 * time.Second)

		nodes[3] = startNode(4)
		time.Sleep(4 * time.Second)
	}, t)

	if !pass {
		t.Fatalf("Linearizability check fail")
	}

	stopNode(nodes...)
	stopMaster(masters...)
}


func TestLinearizabilityRestartTimeout(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(8*time.Second)

	pass := crudWithLinearCheck(10, func() {
		netw.SetUnreliable(10)

		stopMaster(masters[0], masters[1])
		time.Sleep(1 * time.Second)
		masters[0] = startMaster(0)
		masters[1] = startMaster(1)
		time.Sleep(2 * time.Second)

		stopNode(nodes[0])
		time.Sleep(3 * time.Second)

		stopNode(nodes[1])
		time.Sleep(3 * time.Second)

		nodes[0] = startNode(1)
		time.Sleep(3 * time.Second)

		nodes[1] = startNode(2)
		time.Sleep(3 * time.Second)

		stopNode(nodes[2])
		time.Sleep(3 * time.Second)

		stopNode(nodes[3])
		time.Sleep(3 * time.Second)

		nodes[2] = startNode(3)
		time.Sleep(3 * time.Second)

		nodes[3] = startNode(4)
		time.Sleep(4 * time.Second)
	}, t)

	if !pass {
		t.Fatalf("Linearizability check fail")
	}

	stopNode(nodes...)
	stopMaster(masters...)
}

func TestLinearizabilityJoinLeave(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(5)
	time.Sleep(3*time.Second)
	cli := client.MakeMrKVClient(masterAddrs)

	cli.Join(300, []int{1, 2, 4})
	time.Sleep(5*time.Second)

	pass := crudWithLinearCheck(10, func() {

		cli := makeMasterClient()

		fmt.Println("###################")

		for cli.Join(map[int][]int{100: {1, 4, 5}, 200: {2, 3, 4},}) != common.OK {
			t.Errorf("join: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(123)
		time.Sleep(5*time.Second)

		for cli.Leave([]int{200}) != common.OK {
			t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(13)
		time.Sleep(5*time.Second)

		for  cli.Leave([]int{100}) != common.OK {
			t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(3)
		time.Sleep(5*time.Second)

		for cli.Join(map[int][]int{200: {1, 2, 3}}) != common.OK {
			t.Errorf("join: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(23)
		time.Sleep(5*time.Second)

		for cli.Join(map[int][]int{100: {2, 3, 5}}) != common.OK {
			t.Errorf("join: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(123)
		time.Sleep(5*time.Second) // 83

		for cli.Leave([]int{200}) != common.OK {
			t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(13)
		time.Sleep(5*time.Second)

		for cli.Leave([]int{100}) != common.OK {
			t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		fmt.Println(3)
		time.Sleep(5*time.Second)

	}, t)

	if !pass {
		t.Fatalf("Linearizability check fail")
	}

	stopNode(nodes...)
	stopMaster(masters...)
}

