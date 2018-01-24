package store

import (
	"os"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Store struct {
	path string
	db *leveldb.DB
	options *opt.Options
	writeOptions *opt.WriteOptions
	readOptions *opt.ReadOptions
}

/**
 * Функция New инициализирует новое локальное хранилище.
 *
 * @param path string путь в файловой системе.
 * @param reset bool сброс хранилища.
 * @param compression bool сжатие блоков (Snappy).
 *
 * @return s *Store
 * @return err error
 */
func New(path string, reset bool, compression bool) (*Store, error) {
	options := &opt.Options{
		Filter: filter.NewBloomFilter(10),
		Compression: opt.NoCompression,
	}

	writeOptions := &opt.WriteOptions{
		Sync: false,
	}

	if reset {
		os.RemoveAll(path)
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
		path: path,
		db: db,
		options: options,
		writeOptions: writeOptions,
		readOptions: readOptions,
	}

	return s, nil
}

/**
 * Функция Put добавляет ключ и значение в локальную БД.
 *
 * @param key []byte ключ (байт-массив)
 * @param value []byte значение (байт-массив)
 * @return error
 */
func (s *Store) Put(key, value []byte) error {
	return s.db.Put(key, value, s.writeOptions)
}

/**
 * Функция Get получает блок из локальной БД.
 *
 * @param key []byte ключ (байт-массив)
 * @return []byte
 * @return error
 */
func (s *Store) Get(key []byte) ([]byte, error) {
	return s.db.Get(key, s.readOptions)
}

/**
 * Функция Has проверяет есть ли блок в БД.
 *
 * @param key []byte ключ (байт-массив)
 * @return bool
 * @return error
 */
func (s *Store) Has(key []byte) (bool, error) {
	return s.db.Has(key, s.readOptions)
}

/**
 * Функция Delete удаляет блок из локальной БД.
 *
 * @param key []byte ключ (байт-массив)
 * @return error
 */
func (s *Store) Delete(key []byte) error {
	return s.db.Delete(key, s.writeOptions)
}

/**
 * Функция Iterator возвращает инициализированный Iterator для локальной БД.
 *
 * @return iterator.Iterator
 */
func (s *Store) Iterator() iterator.Iterator {
	return s.db.NewIterator(nil, nil)
}

/**
 * Функция HotReset выполняет очистку локальной БД.
 *
 * @return error
 */
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