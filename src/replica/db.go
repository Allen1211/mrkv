package replica

type Store interface {

	Get(key string) ([]byte, error)
	Put(key string, val []byte) error
	Append(key string, val []byte) error
	Delete(key string) error
	Size(prefixes []string) (int64, error)
	FileSize() int64
	Sync() error

	Snapshot() ([]byte, error)
	SnapshotShard(shardId int) ([]byte, error)

	ApplySnapshot(snapshot []byte) error

	Clear(prefix string) error

	Close()

	DeleteFile()
}

const KeyNodeGroup = "Node:Groups"
const KeyNodeForSync = "Node:Sync"

const KeyReplicaPrefix = "Replica:%d"
const KeyLastApplied = "Replica:%d:LastApplied"
const KeyCurrConfig = "Replica:%d:CurrConfig"
const KeyPrevConfig = "Replica:%d:PrevConfig"
const KeyStatus = "Replica:%d:Status"
const KeyForSync = "Replica:%d:Sync"

const (
	ShardBasePrefix = "%dS:"
	ShardUserDataPrefix = "%dS:U:"
	ShardUserDataPattern = "%dS:U:%s"
	ShardMetaPrefix = "%dS:M:%s"
	ShardDupPrefix = "%dS:D:%d"
	ShardVersion = "%dS:V"
)

