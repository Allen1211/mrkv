package raft

import (
	"fmt"
	"testing"
)

func makeAndRecover(t *testing.T, filename string, requireExist bool) *WAL {
	wal, err := MakeWAL(filename, requireExist, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(wal)

	ch := make(chan LogStruct)
	go func() {
		for log := range ch {
			fmt.Println(log)
			if log.Body == nil {
				break
			}
		}
	}()
	if err := wal.Recover(ch); err != nil {
		t.Fatal(err)
	}

	return wal
}

func TestMakeWAL(t *testing.T) {
	wal, err := MakeWAL("E:\\MyWorkPlace\\Project\\6.824\\logs\\test\\logfile1", false, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(wal)

	ch := make(chan LogStruct)
	go func() {
		for log := range ch {
			fmt.Println(log)
			if log.Body == nil {
				break
			}
		}
	}()
	if err := wal.Recover(ch); err != nil {
		t.Fatal(err)
	}
}

func TestWAL_Append(t *testing.T) {
	wal := makeAndRecover(t, "E:\\MyWorkPlace\\Project\\6.824\\logs\\test\\logfile1", false)

	// wal.TruncateTo(10561)
	for i := 0; i < 1200; i++ {
		if _, _, err := wal.Append([]byte(fmt.Sprintf("log%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}

}

func TestWAL_Checkpoint(t *testing.T) {
	wal := makeAndRecover(t, "E:\\MyWorkPlace\\Project\\6.824\\logs\\test\\logfile1", false)

	if err := wal.Checkpoint(32064, 2, 200); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1200; i++ {
		if _, _, err := wal.Append([]byte(fmt.Sprintf("log%d", i))); err != nil {
			t.Fatal(err)
			// if err != ErrLogFileFull {
			// }
		}
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}

}