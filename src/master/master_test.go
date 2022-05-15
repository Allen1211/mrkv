package master

import (
	"fmt"
	"testing"
	"time"

	"mrkv/src/netw"
)

func Test1(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	clerk := MakeClerk(masters)

	reply := clerk.Heartbeat(4, "127.0.0.1:8400", map[int]*GroupInfo{})
	// fmt.Println(reply)
	// clerk.Join(map[int][]int{
	// 	// 100: []int{1,2,3},
	// 	200: []int{2,3,4},
	// })
	reply = clerk.Heartbeat(1, "127.0.0.1:8100", map[int]*GroupInfo{})
	fmt.Println(reply)
	reply = clerk.Heartbeat(2, "127.0.0.1:8200", map[int]*GroupInfo{})
	fmt.Println(reply)
	reply = clerk.Heartbeat(3, "127.0.0.1:8300", map[int]*GroupInfo{})
	fmt.Println(reply)
	reply = clerk.Heartbeat(4, "127.0.0.1:8400", map[int]*GroupInfo{})
	fmt.Println(reply)

	// clerk.Leave([]int{200})
}


func Test2(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	clerk := MakeClerk(masters)

	for i := 0; i < 5; i++ {
		clerk.Join(map[int][]int{
			100: {1, 2, 5},
			200: {2, 3, 4},
		})
		time.Sleep(8*time.Second)

		clerk.Leave([]int{200})
		time.Sleep(8*time.Second)

		clerk.Join(map[int][]int{
			200: {1, 2, 3},
			300: {3, 4, 5},
		})
		time.Sleep(8*time.Second)

		clerk.Leave([]int{100})
		time.Sleep(8*time.Second)

		clerk.Join(map[int][]int{
			100: {1, 4, 5},
		})
		time.Sleep(8*time.Second)

		clerk.Leave([]int{100, 200, 300})
		time.Sleep(8*time.Second)
	}
}

func Test3(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	clerk := MakeClerk(masters)
	// conf := clerk.Query(-1)
	// fmt.Println(conf)
	// clerk.Join(map[int][]int{
	// 	100: {1, 2, 3},
	// 	200: {2, 3, 4},
	// 	300: {3, 4, 5},
	// })

	// clerk.Leave([]int{100, 200, 300})

	// clerk.Leave([]int{100})

	clerk.Leave([]int{200, 300})
	//
	// clerk.Join(map[int][]int{
	// 	100: {1, 2, 3},
	// 	200: {2, 3, 4},
	// })
	//
	// clerk.Join(map[int][]int{
	// 	300: {3,4,5},
	// })
}

func Test4(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	clerk := MakeClerk(masters)
	// conf := clerk.Query(-1)
	// fmt.Println(conf)
	//
	clerk.Join(map[int][]int{
		100: {1, 2, 5},
		200: {2, 3, 4},
		300: {1, 4, 5},
	})

	// clerk.Leave([]int{100, 200, 300})

	// clerk.Leave([]int{100, 200})
	//
	// clerk.Join(map[int][]int{
	// 	100: {1, 2, 3},
	// 	200: {2, 3, 4},
	// })

	// clerk.Join(map[int][]int{
	// 	300: {3,4,5},
	// })
}

func Test5(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	clerk := MakeClerk(masters)

	clerk.Join(map[int][]int{
		300: {1, 2, 4},
	})
	fmt.Println(3)
	time.Sleep(5*time.Second)

	for i := 0; i < 5; i++ {
		fmt.Println("###################")

		clerk.Join(map[int][]int{
			100: {1, 4, 5},
			200: {2, 3, 4},
		})
		fmt.Println(123)
		time.Sleep(5*time.Second)

		clerk.Leave([]int{200})
		fmt.Println(13)
		time.Sleep(5*time.Second)

		clerk.Leave([]int{100})
		fmt.Println(3)
		time.Sleep(5*time.Second)

		clerk.Join(map[int][]int{
			200: {1, 2, 3},
		})
		fmt.Println(23)
		time.Sleep(5*time.Second)

		clerk.Join(map[int][]int{
			100: {2, 3, 5},
		})
		fmt.Println(123)
		time.Sleep(5*time.Second) // 83

		clerk.Leave([]int{200})   // 84
		fmt.Println(13)
		time.Sleep(5*time.Second)

		clerk.Leave([]int{100})
		fmt.Println(3)
		time.Sleep(5*time.Second)
	}
}