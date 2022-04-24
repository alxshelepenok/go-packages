package store

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"os"
)

type Store struct {
	path         string
	db           *leveldb.DB
	options      *opt.Options
	writeOptions *opt.WriteOptions
	readOptions  *opt.ReadOptions
}

func New(path string, reset bool, compression bool) (*Store, error) {
	options := &opt.Options{
		Filter:      filter.NewBloomFilter(10),
		Compression: opt.NoCompression,
	}

	writeOptions := &opt.WriteOptions{
		Sync: false,
	}

	if reset {
		err := os.RemoveAll(path)
		if err != nil {
			return nil, err
		}
		writeOptions.Sync = true
	}

	if compression {
		options.Compression = opt.SnappyCompression
	}

	readOptions := &opt.ReadOptions{
		Strict: opt.DefaultStrict,
	}

	db, err := leveldb.OpenFile(path, options)
	if err != nil {
		return nil, err
	}

	s := &Store{
		path:         path,
		db:           db,
		options:      options,
		writeOptions: writeOptions,
		readOptions:  readOptions,
	}

	return s, nil
}

func (s *Store) Put(key, value []byte) error {
	return s.db.Put(key, value, s.writeOptions)
}

func (s *Store) Get(key []byte) ([]byte, error) {
	return s.db.Get(key, s.readOptions)
}

func (s *Store) Has(key []byte) (bool, error) {
	return s.db.Has(key, s.readOptions)
}

func (s *Store) Delete(key []byte) error {
	return s.db.Delete(key, s.writeOptions)
}

func (s *Store) Iterator() iterator.Iterator {
	return s.db.NewIterator(nil, nil)
}

func (s *Store) HotReset() error {
	if err := s.db.Close(); err != nil {
		return err
	}

	if err := os.RemoveAll(s.path); err != nil {
		return err
	}

	db, err := leveldb.OpenFile(s.path, s.options)
	if err != nil {
		return err
	}

	s.db = db

	return nil
}
