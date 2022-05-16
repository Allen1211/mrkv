package replica

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/allen1211/mrkv/pkg/common/labgob"
	"github.com/allen1211/mrkv/pkg/common/utils"
)

type LevelStore struct {
	mu 		*sync.RWMutex
	db		*leveldb.DB
	path	string
	keyForSync string
}

func MakeLevelStore(path string, keyForSync string) (*LevelStore, error) {
	lvs := new(LevelStore)
	lvs.mu = &sync.RWMutex{}
	lvs.path = path
	lvs.keyForSync = keyForSync

	var err error

	if err = utils.CheckAndMkdir(path); err != nil {
		return nil, err
	}

	options := opt.Options {
		WriteBuffer: 4096 * 1024,
		NoSync: true,
	}
	if lvs.db, err = leveldb.OpenFile(path, &options); err != nil {
		return nil, err
	}

	return lvs, nil
}

func (lvs *LevelStore) Get(key string) ([]byte, error) {
	val, err := lvs.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else {
		return val, err
	}
}

func (lvs *LevelStore) Put(key string, val []byte) error {
	err := lvs.db.Put([]byte(key), val, nil)
	return err
}

func (lvs *LevelStore) Append(key string, val []byte) error {

	currVal, err := lvs.Get(key)
	if err != nil {
		return  err
	}
	if currVal == nil {
		return lvs.Put(key, val)
	} else {
		currVal = append(currVal, val...)
		return lvs.Put(key, currVal)
	}
}

func (lvs *LevelStore) Delete(key string) error  {
	return lvs.db.Delete([]byte(key), nil)
}

func (lvs *LevelStore) Size(prefixes []string) (int64, error) {
	res := int64(0)
	ranges := make([]util.Range, len(prefixes))
	for i, prefix := range prefixes {
		ranges[i] = *util.BytesPrefix([]byte(prefix))
	}
	sizes, err := lvs.db.SizeOf(ranges)
	if err != nil {
		return 0, err
	}
	for _, size := range sizes {
		res += size
	}
	return res, nil
}

func (lvs *LevelStore) Snapshot() ([]byte, error) {
	snapshot, err := lvs.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Release()

	iter := snapshot.NewIterator(nil, nil)
	defer iter.Release()

	return lvs.dumpIter(iter)
}

func (lvs *LevelStore) SnapshotShard(shardId int) ([]byte, error) {
	snapshot, err := lvs.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Release()

	prefix := []byte(fmt.Sprintf(ShardBasePrefix, shardId))
	iter := snapshot.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	return lvs.dumpIter(iter)
}

func (lvs *LevelStore) dumpIter(iter iterator.Iterator) ([]byte, error) {
	var err error
	mp := map[string]string{}

	for iter.First(); iter.Valid(); iter.Next() {
		key, val := iter.Key(), iter.Value()
		mp[string(key)] = string(val)
	}

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err = encoder.Encode(mp)
	return buf.Bytes(), err
}

func (lvs *LevelStore) ApplySnapshot(snapshot []byte) error {
	mp := map[string]string{}
	decoder := labgob.NewDecoder(bytes.NewReader(snapshot))
	if err := decoder.Decode(&mp); err != nil {
		return err
	}
	batch := leveldb.Batch{}
	for key, val := range mp {
		batch.Put([]byte(key), []byte(val))
	}
	return lvs.db.Write(&batch, nil)
}

func (lvs *LevelStore) Clear(prefix string) error {
	iter := lvs.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := lvs.db.Delete(iter.Key(), &opt.WriteOptions{Sync: false}); err != nil {
			return err
		}
	}
	lvs.Sync()

	return nil
}

func (lvs*LevelStore) Sync() error {
	return lvs.db.Put([]byte(lvs.keyForSync),[]byte(""), &opt.WriteOptions{Sync: true})
}

func (lvs *LevelStore) Close() {
	lvs.db.Close()
}

func (lvs *LevelStore) DeleteFile()  {
	utils.DeleteDir(lvs.path)
}

func (lvs *LevelStore) FileSize() int64  {
	return utils.SizeOfDir(lvs.path)
}

func (lvs *LevelStore) Batch() Batch {
	return &LevelBatch{
		b: new(leveldb.Batch),
		db: lvs.db,
	}
}

type LevelBatch struct {
	db  *leveldb.DB
	b	*leveldb.Batch
}

func (batch *LevelBatch) Put(key string, val []byte) {
	batch.b.Put([]byte(key), val)
}

func (batch *LevelBatch) Delete(key string) {
	batch.b.Delete([]byte(key))
}

func (batch *LevelBatch) Execute() error {
	return batch.db.Write(batch.b, nil)
}
