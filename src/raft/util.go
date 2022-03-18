package raft

import "log"

func Assert(predict bool, format string, a ...interface{}) {
	if !predict {
		log.Panicf("assertion failed: " + format, a...)
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}