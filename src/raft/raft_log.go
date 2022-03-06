package raft

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"mrkv/src/common/labgob"
)

type raftlog interface {
	CpIdx()				int
	CpTerm()			int
	CpLSN()				uint64
	checkpoint()		(int, int)
	idxAt(idx int)		LogEntry
	posAt(pos int) 		LogEntry
	first()				LogEntry
	last()				LogEntry
	lastIdxTerm()		(int, int)
	slice(from, to int) []LogEntry
	length()			int
	append(ents ...LogEntry) 		bool
	truncateAppend(ents ...LogEntry) bool

	commitTo(idx int)
	applyTo(idx int)
	compactTo(idx int, term int, lsn uint64)

	dump() []byte
	restore(data []byte)

	sync()
}

type inMemoryRaftLog struct {
	logger	*logrus.Logger

	cpIdx 	int
	cpTerm 	int
	logs 	[]LogEntry
}

func makeInMemoryRaftLog(cpIdx, cpTerm int, rf *Raft) raftlog {
	rl := new(inMemoryRaftLog)
	rl.cpIdx = cpIdx
	rl.cpTerm = cpTerm
	rl.logs = make([]LogEntry, 0)
	rl.logger = rf.logger
	return rl
}

func (rl *inMemoryRaftLog) CpIdx() int {
	return rl.cpIdx
}

func (rl *inMemoryRaftLog) CpTerm() int {
	return rl.cpTerm
}

func (rl *inMemoryRaftLog) CpLSN() uint64 {
	return 0
}

func (rl *inMemoryRaftLog) checkpoint() (int, int) {
	return rl.cpIdx, rl.cpTerm
}

func (rl *inMemoryRaftLog) idxAt(idx int) LogEntry {
	pos := idx - rl.cpIdx - 1
	return rl.posAt(pos)
}

func (rl *inMemoryRaftLog) posAt(pos int) LogEntry {
	return rl.logs[pos]
}

func (rl *inMemoryRaftLog) first() LogEntry {
	return rl.logs[0]
}

func (rl *inMemoryRaftLog) last() LogEntry {
	return rl.logs[len(rl.logs)-1]
}

func (rl *inMemoryRaftLog) lastIdxTerm() (int, int) {
	if rl.length() == 0 {
		return rl.cpIdx, rl.cpTerm
	} else {
		last := rl.last()
		return last.Index, last.Term
	}
}

func (rl *inMemoryRaftLog) slice(from, to int) []LogEntry {
	if to != -1 {
		return rl.logs[from:to]
	} else {
		return rl.logs[from:]
	}
}

func (rl *inMemoryRaftLog) length() int {
	return len(rl.logs)
}

func (rl *inMemoryRaftLog) append(ents ...LogEntry) bool {
	rl.logs = append(rl.logs, ents...)
	return true
}

func (rl *inMemoryRaftLog) truncateAppend(ents ...LogEntry) bool {
	flag := false
	for _, e := range ents {
		if rl.posOf(e.Index) < 0 {
			continue
		}
		if rl.posOf(e.Index) < rl.length() && rl.idxAt(e.Index).Term != e.Term {
			flag = true
		}
		if rl.posOf(e.Index) < rl.length()  {
			if flag {
				rl.logs[rl.posOf(e.Index)] = e
			}
		} else {
			rl.logs = append(rl.logs, e)
		}
	}
	return true
}

func (rl *inMemoryRaftLog) commitTo(idx int) {
}

func (rl *inMemoryRaftLog) applyTo(idx int) {
}

func (rl *inMemoryRaftLog) compactTo(idx int, term int, lsn uint64) {
	lastLogIdx, lastLogTerm := rl.lastIdxTerm()
	if term == -1 {
		if idx > lastLogIdx {
			term = lastLogTerm
		} else {
			term = rl.idxAt(idx).Term
		}
	}
	if idx > lastLogIdx {
		rl.logs = make([]LogEntry, 0)
	} else {
		rl.logs = rl.slice(idx - rl.cpIdx, -1)
	}
	rl.cpIdx = idx
	rl.cpTerm = term
}

func (rl *inMemoryRaftLog) dump() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rl.logs)
	e.Encode(rl.cpIdx)
	e.Encode(rl.cpTerm)
	data := w.Bytes()
	return data
}

func (rl *inMemoryRaftLog) restore(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cpIdx, cpTerm int
	var logs []LogEntry
	if d.Decode(&logs) != nil ||
		d.Decode(&cpIdx) != nil ||
		d.Decode(&cpTerm) != nil {
		log.Fatalln("fail to decode log state")
	} else {
		rl.logs = logs
		rl.cpIdx = cpIdx
		rl.cpTerm = cpTerm
	}

	// fmt.Printf("restore log: %v\n", rl.logs)
}

func (rl *inMemoryRaftLog) sync()  {

}

func (rl *inMemoryRaftLog) posOf(idx int) int {
	return idx - rl.cpIdx - 1
}

func (rl *inMemoryRaftLog) idxOf(pos int) int {
	return pos + rl.cpIdx + 1
}

type WriteAheadLogEntry struct {
	StartLSN 	uint64
	EndLSN	 	uint64
	LogEntry
}

type writeAheadRaftLog struct {
	logger	*logrus.Logger

	mu 		sync.RWMutex

	cpIdx 	int
	cpTerm 	int
	logs 	[]WriteAheadLogEntry

	wal		*WAL
	rf		*Raft
	logFileName string
	logFileCap  uint64
}

func makeWriteAheadRaftLog(cpIdx, cpTerm int, rf *Raft, logFileName string, logFileCap uint64) raftlog {
	rl := new(writeAheadRaftLog)
	rl.logger = rf.logger
	rl.cpIdx = cpIdx
	rl.cpTerm = cpTerm
	rl.logs = make([]WriteAheadLogEntry, 0)
	rl.rf = rf
	rl.logFileName = logFileName
	rl.logFileCap = logFileCap
	return rl
}

func (rl *writeAheadRaftLog) CpIdx() int {
	return rl.cpIdx
}

func (rl *writeAheadRaftLog) CpTerm() int {
	return rl.cpTerm
}

func (rl *writeAheadRaftLog) CpLSN() uint64 {
	return rl.wal.cpLSN
}

func (rl *writeAheadRaftLog) checkpoint() (int, int) {
	return rl.cpIdx, rl.cpTerm
}

func (rl *writeAheadRaftLog) idxAt(idx int) LogEntry {

	return rl.idxAtNoLock(idx)
}

func (rl *writeAheadRaftLog) idxAtNoLock(idx int) LogEntry {

	pos := idx - rl.cpIdx - 1
	return rl.logs[pos].LogEntry
}

func (rl *writeAheadRaftLog) posAt(pos int) LogEntry {

	return rl.logs[pos].LogEntry
}

func (rl *writeAheadRaftLog) first() LogEntry {
	return rl.logs[0].LogEntry
}

func (rl *writeAheadRaftLog) last() LogEntry {
	return rl.lastNoLock()
}

func (rl *writeAheadRaftLog) lastNoLock() LogEntry {
	return rl.logs[len(rl.logs)-1].LogEntry
}

func (rl *writeAheadRaftLog) lastIdxTerm() (int, int) {

	return rl.lastIdxTermNoLock()
}

func (rl *writeAheadRaftLog) lastIdxTermNoLock() (int, int) {

	if len(rl.logs) == 0 {
		return rl.cpIdx, rl.cpTerm
	} else {
		last := rl.lastNoLock()
		return last.Index, last.Term
	}
}

func (rl *writeAheadRaftLog) slice(from, to int) []LogEntry {

	ents := rl.doSlice(from, to)
	res := make([]LogEntry, len(ents))
	for i, e := range ents {
		res[i] = e.LogEntry
	}
	return res
}

func (rl *writeAheadRaftLog) doSlice(from, to int) []WriteAheadLogEntry {
	if to != -1 {
		return rl.logs[from:to]
	} else {
		return rl.logs[from:]
	}
}

func (rl *writeAheadRaftLog) length() int {

	return len(rl.logs)
}

func (rl *writeAheadRaftLog) append(ents ...LogEntry) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	var err error
	lgs := make([]WriteAheadLogEntry, len(ents))
	buf := new(bytes.Buffer)
	for i, e := range ents {
		encoder := labgob.NewEncoder(buf)
		if err = encoder.Encode(e); err != nil {
			log.Fatal(err)
		}
		var startLSN, length uint64
		retry:
		if startLSN, length,  err = rl.wal.Append(buf.Bytes()); err != nil {
			if err != ErrLogFileFull {
				log.Fatal(err)
			}
			cpAtLeastLSN := rl.wal.cpLSN + (length - rl.wal.remain())
			rl.mu.Unlock()
			// rl.rf.mu.Unlock()

			rl.rf.checkpointCh <- rl.cpIdx
			rl.logger.Infof("RaftLog: start waiting checkpoint to %d", cpAtLeastLSN)
			if !rl.waitCheckpointTo(cpAtLeastLSN) {
				rl.logger.Infof("wait checkpoint to %d timeout", cpAtLeastLSN)
			}
			// rl.rf.mu.Lock()
			rl.mu.Lock()
			// Assert(rl.rf.lastApplied > rl.cpIdx, "lastApplied %d <= cpIdx %d but log file is full", rl.rf.lastApplied, rl.cpIdx)
			goto retry
		}

		lgs[i].LogEntry = e
		lgs[i].StartLSN = startLSN
		lgs[i].EndLSN = startLSN + length
	}
	rl.logs = append(rl.logs, lgs...)

	return true
}

func (rl *writeAheadRaftLog) truncateAppend(ents ...LogEntry) bool {
	x, y := -1, -1
	for i, e := range ents {
		if rl.posOf(e.Index) < 0 {
			continue
		}
		if x == -1 && rl.posOf(e.Index) < rl.length() && rl.idxAt(e.Index).Term != e.Term {
			x = i
		}
		if rl.posOf(e.Index) == rl.length() {
			y = i
			break
		}
	}

	rl.mu.Lock()
	if x != -1 {
		xindex := ents[x].Index
		lsn := rl.logs[rl.posOf(xindex)].StartLSN

		rl.wal.TruncateTo(lsn)
		rl.logs = rl.logs[:rl.posOf(xindex)]

		rl.mu.Unlock()
		return rl.append(ents[x:]...)
	} else if y != -1 {
		rl.mu.Unlock()
		return rl.append(ents[y:]...)
	} else {
		rl.mu.Unlock()
		return true
	}
}

func (rl *writeAheadRaftLog) commitTo(idx int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if idx <= rl.cpIdx {
		return
	}
	lg := rl.logs[rl.posOf(idx)]
	rl.wal.commitLSN = lg.EndLSN
	rl.logger.Infof("RaftLog %d: commitLSN change to %d", rl.rf.me, rl.wal.commitLSN)
}

func (rl *writeAheadRaftLog) applyTo(idx int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if idx <= rl.cpIdx {
		return
	}
	lg := rl.logs[rl.posOf(idx)]
	rl.wal.applyLSN = lg.EndLSN
}

func (rl *writeAheadRaftLog) compactTo(idx int, term int, lsn uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if idx <= rl.cpIdx {
		return
	}
	if term == -1 {
		term = rl.idxAtNoLock(idx).Term
	}

	lastLogIdx, _ := rl.lastIdxTermNoLock()

	var cpLSN uint64
	if idx > lastLogIdx {
		// cpLSN = rl.logs[len(rl.logs)-1].EndLSN
		cpLSN = lsn
	} else {
		cpLSN = rl.logs[rl.posOf(idx)].EndLSN
	}
	rl.logger.Infof("RaftLog: begin compact to lsn=%d", cpLSN)

	if err := rl.wal.Checkpoint(cpLSN, term, idx); err != nil {
		log.Fatal(err)
	}

	if idx > lastLogIdx {
		rl.logs = []WriteAheadLogEntry{}
	} else {
		rl.logs = rl.doSlice(idx - rl.cpIdx, -1)
	}

	rl.cpIdx = idx
	rl.cpTerm = term

}

func (rl *writeAheadRaftLog) dump() []byte {
	return []byte(rl.wal.Filename)
}

func (rl *writeAheadRaftLog) restore(data []byte) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	var err error
	var logFileName = string(data)
	rl.logger.Infof("RaftLog %d restore from log file %s", rl.rf.me, logFileName)

	if rl.wal, err = MakeWAL(logFileName, false, 0, rl.logger); err != nil {
		log.Fatal(err)
	}

	ch := make(chan LogStruct, 1000)
	go func() {
		if err = rl.wal.Recover(ch); err != nil {
			log.Fatal(err)
		}
	}()
	for lg := range ch {
		if lg.Body == nil {
			break
		}
		ent := WriteAheadLogEntry {
			StartLSN: lg.LSN,
			EndLSN: rl.wal.endLsnOf(&lg),
		}
		decoder := labgob.NewDecoder(bytes.NewReader(lg.Body))
		if err = decoder.Decode(&ent.LogEntry); err != nil {
			log.Fatal(err)
		}
		rl.logs = append(rl.logs, ent)
		rl.logger.Debugf("RaftLog: recover log : %v", ent)
	}
	rl.cpIdx, rl.cpTerm = int(rl.wal.cpIdx), int(rl.wal.cpTerm)
	rl.logger.Infof("RaftLog: recover finish: cpIdx=%d cpTerm=%d", rl.cpIdx, rl.cpTerm)
}

func (rl *writeAheadRaftLog) sync()  {
	// if err := rl.wal.Sync(); err != nil {
	// 	log.Fatal(err)
	// }
}

func (rl *writeAheadRaftLog) posOf(idx int) int {
	return idx - rl.cpIdx - 1
}

func (rl *writeAheadRaftLog) idxOf(pos int) int {
	return pos + rl.cpIdx + 1
}

func (rl *writeAheadRaftLog) waitCheckpointTo(lsn uint64) bool {
	maxWaitMs := 2000
	waitMs := 100
	for maxWaitMs > 0 {
		nowCpLsn := rl.wal.cpLSN
		if nowCpLsn >= lsn {
			return true
		}
		time.Sleep(time.Duration(waitMs)*time.Millisecond)
		maxWaitMs -= waitMs
	}
	return false
}