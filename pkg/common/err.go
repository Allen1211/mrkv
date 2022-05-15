package common

//go:generate msgp

type Err string

const (
	OK                 Err = "OK"
	ErrNoKey           Err = "ErrNoKey"
	ErrWrongGroup      Err = "ErrWrongGroup"
	ErrWrongLeader     Err = "ErrWrongLeader"
	ErrFailed          Err = "ErrFailed"
	ErrDuplicate       Err = "ErrDuplicate"
	ErrDiffConf        Err = "ErrDiffConf"
	ErrNodeNotRegister Err = "node not register"
	ErrNodeClosed      Err = "ErrNodeClosed"
)