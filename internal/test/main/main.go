package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/allen1211/mrkv/internal/test"
)

func main() {
	args := os.Args
	if len(args) == 1 {
		fmt.Printf("%s [perf/...]\n", args[0])
		os.Exit(1)
	}
	program := args[1]
	if program == "perf" {
		runPerformanceTest(args[2:])
	} else {
		fmt.Printf("%s [perf/...]\n", args[0])
		os.Exit(1)
	}

}

func runPerformanceTest(args []string) {
	var total, length, threads int
	var masterStr string
	var masters []string
	var testFunc string
	flagSet := flag.NewFlagSet("perf", flag.ExitOnError)
	flagSet.StringVar(&masterStr, "masters", "", "master servers")
	flagSet.IntVar(&total, "total", -1, "total read/write")
	flagSet.IntVar(&length, "length", 100, "value length in bytes")
	flagSet.IntVar(&threads, "thread", 1, "number of test threads")
	flagSet.StringVar(&testFunc, "test", "", "prepare/read_only/write_only/read_write")
	flagSet.Parse(args)

	if masterStr == "" {
		fmt.Printf("require argument masters\n")
		os.Exit(1)
	}
	masters = strings.Split(masterStr, ",")
	if len(masters) <= 1 {
		fmt.Printf("not enough master servers, given: %d, require at least: %d\n", len(masters), 2)
		os.Exit(1)
	}
	if testFunc == "" {
		fmt.Printf("require test function: prepare,read_only,write_only or read_write\n")
		os.Exit(1)
	}

	performanceTest := test.MakePerformanceTest(masters, threads, length, total)
	switch testFunc {
	case "prepare":
		performanceTest.Prepare()
	case "read_only":
		performanceTest.TestReadOnly()
	case "write_only":
		performanceTest.TestWriteOnly()
	case "read_write":
		performanceTest.TestReadWrite()
	}
}