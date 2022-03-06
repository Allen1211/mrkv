package replica

type Store interface {

	Get(key string) ([]byte, error)
	Put(key string, val []byte) error
	Append(key string, val []byte) error

	Snapshot() ([]byte, error)
	SnapshotShard(shardId int) ([]byte, error)

	ApplySnapshot(snapshot []byte) error

	Clear(prefix string) error

	Close()
}

const KeyLastApplied = "Server:LastApplied"
const KeyCurrConfig = "Server:CurrConfig"
const KeyPrevConfig = "Server:PrevConfig"

const (
	ShardBasePrefix = "%d:"
	ShardUserDataPrefix = "%d:U:"
	ShardUserDataPattern = "%d:U:%s"
	ShardMetaPrefix = "%d:M:%s"
	ShardDupPrefix = "%d:D:%d"
)

