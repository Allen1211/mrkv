package common

const (
	CmdTypeEmpty  uint8 =  iota
	CmdTypeKV
	CmdTypeSnap
	CmdTypeConf
	CmdTypeInstallShard
	CmdTypeEraseShard
	CmdTypeStopWaiting

	CmdTypeHeartbeat
	CmdTypeShow
	CmdTypeQuery
	CmdTypeJoin
	CmdTypeLeave
	CmdTypeMove
)

//go:generate msgp

type CmdWrap struct {
	Type	uint8
	Body	[]byte
}