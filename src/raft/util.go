package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}){
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Assert(predict bool, format string, a ...interface{}) {
	if !predict {
		log.Panicf("assertion failed: " + format, a...)
	}
}