package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorageReader(db *badger.DB) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		db: db,
	}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.db, cf, key)
	if err != nil && err.Error() == "Key not found" {
		err = nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := r.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (r *StandAloneStorageReader) Close() {
	// r.txn.Discard()
}
