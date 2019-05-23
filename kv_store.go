package simplegraph

type KVStore interface {
	Get(prefix []byte, stream chan<- []byte) error
	Put(keys ... []byte) error
	Delete(keys ... []byte) error
}
