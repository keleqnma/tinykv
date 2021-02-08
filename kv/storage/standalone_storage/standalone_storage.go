package standalone_storage

import (
	"os"
	"strings"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	reader *StandAloneStorageReader
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	if _, err := os.Stat(conf.DBPath); err != nil {
		if os.MkdirAll(conf.DBPath, os.ModePerm) != nil {
			panic(err)
		}
	}
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	if strings.Contains(conf.DBPath, "tmp") {
		defer os.RemoveAll(conf.DBPath)
	}
	return &StandAloneStorage{
		db:     db,
		reader: NewStandAloneStorageReader(db),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s.reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.db, modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			err = engine_util.DeleteCF(s.db, modify.Cf(), modify.Key())
		}
		if err != nil {
			return err
		}
	}
	return nil
}
