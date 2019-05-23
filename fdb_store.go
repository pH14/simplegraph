package simplegraph

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type FdbGraph struct {
	db *fdb.Database
}

func (f *FdbGraph) Get(prefix []byte, outputStream chan<- []byte) error {
	defer close(outputStream)

	_, e := f.db.ReadTransact(func(transaction fdb.ReadTransaction) (i interface{}, e error) {
		prefixRange, e := fdb.PrefixRange(prefix)

		if e != nil {
			return nil, e
		}

		rangeIterator := transaction.GetRange(prefixRange, fdb.RangeOptions{}).Iterator()

		for rangeIterator.Advance() {
			kv, e := rangeIterator.Get()

			if e != nil {
				return nil, e
			}

			outputStream <- kv.Key
		}

		return nil, nil
	})

	return e
}

func (f *FdbGraph) Put(keys ...[]byte) error {
	emptyByte := make([]byte, 0)

	_, e := f.db.Transact(func(txn fdb.Transaction) (i interface{}, e error) {
		for _, key := range keys {
			txn.Set(fdb.Key(key), fdb.Key(emptyByte))
		}

		return nil, nil
	})

	return e
}

func (f *FdbGraph) Delete(keys ...[]byte) error {
	_, e := f.db.Transact(func(txn fdb.Transaction) (i interface{}, e error) {
		for _, key := range keys {
			txn.Clear(fdb.Key(key))
		}

		return nil, nil
	})

	return e
}

