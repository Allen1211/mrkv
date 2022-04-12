package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	OK			  error = nil
	ErrLogFileNotExist  = errors.New("log file not exits")
	ErrLogFileFull	  	= errors.New("log file not enough space")
	ErrLogTooLong     	= errors.New("log too long")
	ErrLogFileHeaderWrongMagic = errors.New("log file header magic not match")
)

const PageSize = 4096

const (
	LogFileMagic uint32 = 0x19283745
	// LogFileCapacityDefault uint64 = 1 << 19
	LogFileCapacityDefault uint64 = 1 << 23
	// LogFileCapacityDefault uint64 = 8192 * 4
)

type LogFileHeader struct {
	Magic			uint32
	Reserve			uint32
	CheckpointLSN	uint64
	CheckpointIndex	int64
	CheckpointTerm	int64
}

func SizeOfLogFileHeader() uint64 {
	return uint64(binary.Size(LogFileHeader{}))
}

const LogHeaderMagic = 0x34761287

type LogHeader struct {
	Magic 	uint32
	LSN		uint64
	Len		uint64
}

func SizeOfLogHeader() uint64 {
	return uint64(binary.Size(LogHeader{}))
}

type LogStruct struct {
	LogHeader
	Body	[]byte
}

func (ls *LogStruct) encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, ls.LogHeader); err != nil {
		return nil, err
	}
	buf.Write(ls.Body)
	return  buf.Bytes(), nil
}

type logScanner struct {
	wal		   *WAL
	lsn        uint64
	acquireLen uint64
	buf        []byte
	flag	   bool
	log		   *LogStruct
	first	   bool
	prevLsn	   uint64
	cpLsn	   uint64
	lastLog	   *LogStruct
}

func MakeLogScanner(wal *WAL, lsn, cpLsn uint64) *logScanner {
	return &logScanner{
		wal: wal,
		lsn: lsn,
		acquireLen: SizeOfLogHeader(),
		buf: make([]byte, 0),
		flag: true,
		log: &LogStruct {
			Body: make([]byte, 0),
		},
		cpLsn: cpLsn,
		first: true,
	}
}

func (s *logScanner) scan(buf []byte, startLsn uint64, ch chan LogStruct) (bool, error) {
	endLSN := startLsn + uint64(len(buf))

	for {
		if s.wal.lsn2ofs(s.lsn) == 0 {
			s.lsn += SizeOfLogFileHeader()
		}
		if s.lsn >= endLSN {
			// should read next page
			return false, nil
		}

		var readLen uint64
		if s.lsn + s.acquireLen > endLSN {
			// can only partial read acquire length
			readLen = endLSN - s.lsn
			s.buf = append(s.buf, buf[s.lsn-startLsn:]...)
			s.lsn += readLen
			s.acquireLen -= readLen
			return false, nil
		} else {
			// can fully read acquire length
			readLen = s.acquireLen
			s.buf = append(s.buf, buf[s.lsn-startLsn:s.lsn-startLsn+s.acquireLen]...)
			s.lsn += readLen
		}

		if s.flag {
			Assert(uint64(len(s.buf)) == SizeOfLogHeader(), "log scanner buffer length %d not eq to logHeader length %d", len(s.buf), SizeOfLogHeader())
			if err := binary.Read(bytes.NewReader(s.buf), binary.LittleEndian, &s.log.LogHeader); err != nil {
				return true, err
			}
			if s.log.Magic != LogHeaderMagic || (s.first && s.log.LSN != s.cpLsn || !s.first && s.log.LSN <= s.prevLsn) {
				return true, nil
			}
			if s.first {
				s.first = false
			}

			s.flag = false
			s.acquireLen = s.log.Len
			s.buf = []byte{}

		} else {
			Assert(uint64(len(s.buf)) == s.log.Len, "log scanner buffer length %d not eq to log body length %d", len(s.buf), s.log.Len)
			s.log.Body = make([]byte, len(s.buf))
			copy(s.log.Body, s.buf)

			ch <- *s.log

			if s.lastLog == nil {
				s.lastLog = s.log
			} else {
				if s.log.LSN < s.lastLog.LSN {
					fmt.Println(1)
				}
				*s.lastLog = *s.log
			}

			s.flag = true
			s.acquireLen = SizeOfLogHeader()
			s.prevLsn = s.log.LSN
			s.buf = []byte{}
			s.log = &LogStruct{
				Body: make([]byte, 0),
			}
		}

	}

}

type WAL struct {
	log  *logrus.Logger
	
	mu		sync.RWMutex

	Filename 		string
	Capacity 		uint64

	file			*os.File
	cpTerm			int64
	cpIdx			int64
	cpLSN			uint64
	applyLSN		uint64
	commitLSN		uint64
	writeLSN		uint64
}

func MakeWAL(filename string, needInit bool, capacity uint64, logger *logrus.Logger) (wal *WAL,err error) {
	wal = new(WAL)
	wal.log = logger
	
	wal.mu = sync.RWMutex{}
	wal.Filename = filename

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		needInit = true
	}
	if needInit {
		if capacity == 0 {
			wal.Capacity = LogFileCapacityDefault
		} else {
			if capacity % PageSize != 0 {
				capacity += PageSize - capacity%PageSize
			}
			wal.Capacity = capacity
		}

		if wal.file, err = os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_CREATE, os.ModePerm); err != nil {
			return nil, err
		}
		if err = wal.file.Truncate(int64(wal.Capacity)); err != nil {
			return nil, err
		}
		header := LogFileHeader {
			Magic: LogFileMagic,
			CheckpointTerm: 0,
			CheckpointIndex: 0,
			CheckpointLSN: SizeOfLogFileHeader(),
		}
		if err = wal.writeFileHeader(header); err != nil {
			return nil, err
		}
	} else {
		if wal.file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm); err != nil {
			return nil, err
		}

		stat, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		wal.Capacity = uint64(stat.Size())


		Assert(stat.Size() == int64(wal.Capacity), "log file actual size %d != wal.Capacity %d", stat.Size(), wal.Capacity)
	}

	return wal, nil
}

func (wal *WAL) Checkpoint(toLSN uint64, term, idx int) error {
	wal.log.Infof("WAL Checkpoint: required toLSN=%d, current cpLSN=%d, applyLSN=%d, commitLSN=%d, writeLSN=%d, remain=%d",
		toLSN, wal.cpLSN, wal.applyLSN, wal.commitLSN, wal.writeLSN, wal.remain())

	if int64(term) < wal.cpTerm {
		wal.log.Infof("WAL Checkpoint: term(%d) <= cpTerm %d, no need to checkpoint", term, wal.cpTerm)
		return nil
	}
	if toLSN <= wal.cpLSN {
		wal.log.Infof("WAL Checkpoint: toLSN(%d) <= cpLSN %d, no need to checkpoint", toLSN, wal.cpLSN)
		return nil
	}

	wal.cpTerm = int64(term)
	wal.cpLSN = toLSN
	wal.log.Infof("WAL Checkpoint: cpLSN advance to %d, cpTerm=%d", toLSN, wal.cpTerm)

	if wal.applyLSN < wal.cpLSN {
		wal.log.Infof("WAL Checkpoint: applyLSN advance to %d", toLSN)
		wal.applyLSN = wal.cpLSN
	}
	if wal.commitLSN < wal.cpLSN {
		wal.log.Infof("WAL Checkpoint: commitLSN advance to %d", toLSN)
		wal.commitLSN = wal.cpLSN
	}
	if wal.writeLSN < wal.cpLSN {
		wal.log.Infof("WAL Checkpoint: writeLSN advance to %d", toLSN)
		wal.writeLSN = wal.cpLSN
	}
	header := LogFileHeader{
		Magic:           LogFileMagic,
		Reserve:         0,
		CheckpointLSN:   wal.cpLSN,
		CheckpointIndex: int64(idx),
		CheckpointTerm:  wal.cpTerm,
	}
	if err := wal.writeFileHeader(header);err != nil {
		return err
	}
	wal.log.Infof("WAL Checkpoint: finished, current cpLSN=%d, applyLSN=%d, commitLSN=%d, writeLSN=%d, remain=%d",
		wal.cpLSN, wal.applyLSN, wal.commitLSN, wal.writeLSN, wal.remain())
	return nil
}

func (wal *WAL) writeFileHeader(header LogFileHeader) error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		wal.log.Errorf("WAL Checkpoint: failed to decode log file header: %v", err)
		return err
	}
	if _, err := wal.file.WriteAt(buf.Bytes(), 0); err != nil {
		wal.log.Errorf("WAL Checkpoint: failed to write log file header to file: %v", err)
		return err
	}
	if err := wal.Sync(); err != nil {
		return err
	}
	return nil
}

func (wal *WAL) TruncateTo(lsn uint64) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if lsn >= wal.writeLSN {
		return
	}
	Assert(lsn >= wal.commitLSN, "truncate lsn %d < commitLSN %d", lsn, wal.commitLSN)
	wal.writeLSN = lsn
	if wal.lsn2ofs(wal.writeLSN) == 0 {
		wal.writeLSN += SizeOfLogFileHeader()
	}

	wal.log.Infof("WAL: truncate to %d\n", lsn)
}

func (wal *WAL) Append(logBody []byte) (lsn uint64, length uint64, err error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if logBody == nil || len(logBody) == 0 {
		return wal.writeLSN, 0, nil
	}
	ls := LogStruct {
		LogHeader: LogHeader {
			Magic: LogHeaderMagic,
			LSN:  wal.writeLSN,
			Len:  uint64(len(logBody)),
		},
		Body: logBody,
	}
	writeData, err := ls.encode()
	if err != nil {
		return wal.writeLSN,0, err
	}

	length = uint64(len(writeData))
	if length > wal.trueCapacity() {
		return wal.writeLSN, length, ErrLogTooLong
	}
	if length > wal.remain() {
		return wal.writeLSN, length, ErrLogFileFull
	}

	writeOffset := wal.lsn2ofs(wal.writeLSN)
	Assert(uint64(writeOffset) >= SizeOfLogFileHeader(), "write offset in log file header")
	if err = wal.doWrite(writeData, writeOffset, int64(length)); err != nil {
		return wal.writeLSN, length, err
	}

	wal.log.Debugf("WAL append log at lsn(%d) offset(%d) length(%d) FileName %s", wal.writeLSN, writeOffset, length, wal.Filename)

	lsn = wal.writeLSN

	if wal.willLoopBack(wal.writeLSN, length) {
		wal.writeLSN += length + SizeOfLogFileHeader()
 	} else {
 		wal.writeLSN += length
	}
	Assert(uint64(wal.lsn2ofs(wal.writeLSN)) >= SizeOfLogFileHeader(), "writeLSN %d in log file header!", wal.writeLSN)

	return
}

func (wal *WAL) Sync() error {
	return wal.file.Sync()
}


func (wal *WAL) Recover(ch chan LogStruct) error {
	header := LogFileHeader{}

	buf := make([]byte, SizeOfLogFileHeader())

	if _, err := wal.file.ReadAt(buf, 0); err != nil {
		return err
	}
	if err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &header); err != nil {
		return err
	}
	if header.Magic != LogFileMagic {
		return ErrLogFileHeaderWrongMagic
	}
	wal.cpLSN = header.CheckpointLSN
	wal.cpTerm = header.CheckpointTerm
	wal.cpIdx = header.CheckpointIndex
	wal.log.Infof("WAL Recover: header recover finish, cpLSN=%d, cpTerm=%d", wal.cpLSN, wal.cpTerm)
	cpOfs := wal.lsn2ofs(wal.cpLSN)
	Assert(uint64(cpOfs) >= SizeOfLogFileHeader(), "cp offset %d in log file header", cpOfs)

	wal.log.Infof("WAL Recover: log scanning begin, FileName %s", wal.Filename)

	scanBufSize := wal.Capacity / 10
	if scanBufSize < PageSize {
		scanBufSize = PageSize
	}

	scanner := MakeLogScanner(wal, wal.cpLSN, wal.cpLSN)

	buf = make([]byte, scanBufSize)
	readLSN := wal.pageAlign(wal.cpLSN)
	outer:
	for {
		wal.log.Infof("WAL Recover: log scanning readLSN %d", readLSN)
		if err := wal.doRead(buf, wal.lsn2ofs(readLSN), int64(len(buf))); err != nil {
			wal.log.Errorf("WAL Recover: log scanning read error: %v", err)
			return err
		}
		for i := 0; i < len(buf) / PageSize; i++ {
			pageBuf := buf[i*PageSize:(i+1)*PageSize]
			if done, err := scanner.scan(pageBuf, readLSN, ch); err != nil {
				wal.log.Errorf("WAL Recover: log scanner error: %v", err)
				return err
			} else if done {
				wal.log.Infof("WAL Recover: log scanning finish")
				ch <- LogStruct {
					Body: nil,
				}
				break outer
			}
			readLSN += PageSize
		}
	}

	lastLog := scanner.lastLog
	if lastLog == nil {
		wal.writeLSN = wal.cpLSN
	} else {
		wal.writeLSN = lastLog.LSN + SizeOfLogHeader() + lastLog.Len
	}
	if wal.lsn2ofs(wal.writeLSN) == 0 {
		wal.writeLSN += SizeOfLogFileHeader()
	}
	wal.log.Infof("WAL Recover: log recover finish, writeLsn=%d", wal.writeLSN)

	return nil
}

func (wal *WAL) doWrite(buf []byte, ofs int64, length int64) error {
	startOfs := int64(SizeOfLogFileHeader())
	endOfs := int64(wal.Capacity)

	if ofs + length > endOfs {
		cutLen := endOfs - ofs
		if _, err := wal.file.WriteAt(buf[:cutLen], ofs); err != nil {
			return err
		}
		remainLen := length - cutLen
		return wal.doWrite(buf[cutLen:], startOfs, remainLen)
	}

	_, err := wal.file.WriteAt(buf, ofs)
	return err
}

func (wal *WAL) doRead(buf []byte, ofs int64, length int64) error {
	endOfs := int64(wal.Capacity)

	if ofs + length > endOfs {
		cutLen := endOfs - ofs
		if _, err := wal.file.ReadAt(buf[:cutLen], ofs); err != nil {
			return err
		}
		remainLen := length - cutLen
		return wal.doRead(buf[cutLen:], 0, remainLen)
	}

	_, err := wal.file.ReadAt(buf, ofs)
	return err
}

func (wal *WAL) remain() uint64 {
	return wal.trueCapacity() - wal.size()
}

func (wal *WAL) size() uint64 {
	return wal.writeLSN - wal.cpLSN
}

func (wal *WAL) trueCapacity() uint64 {
	return wal.Capacity - SizeOfLogFileHeader()
}

func (wal *WAL) lsn2ofs(lsn uint64) int64 {
	// return int64((lsn % wal.trueCapacity()) + SizeOfLogFileHeader())
	return int64(lsn % wal.Capacity)
}

func (wal *WAL) endLsnOf(lg *LogStruct) uint64 {
	if wal.willLoopBack(lg.LSN, lg.Len + SizeOfLogHeader()) {
		return lg.LSN + lg.Len + SizeOfLogHeader() + SizeOfLogFileHeader()
	} else {
		return lg.LSN + lg.Len + SizeOfLogHeader()
	}
}

func (wal *WAL) pageAlign(lsn uint64) uint64 {
	return lsn - (lsn % PageSize)
}

func (wal *WAL) willLoopBack(lsn uint64, step uint64) bool {
	return wal.lsn2ofs(lsn) > wal.lsn2ofs(lsn + step)
}

func (wal *WAL) close() {
	wal.file.Close()
}