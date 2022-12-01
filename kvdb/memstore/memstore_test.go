package memstore

import (
	"reflect"
	"testing"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

func BenchmarkMemstore(b *testing.B) {
	db := New()
	benchmark(b, db)
}

func benchmark(b *testing.B, db kvdb.Store) {
	putOp := func(key []byte, value []byte) {
		err := db.Put(key, value)
		if err != nil {
			b.Error(err)
		}
	}

	loopOp(putOp)

	getOp := func(key []byte, val []byte) {
		v, err := db.Get(key)
		if err != nil {
			b.Error(err)
		}
		if !reflect.DeepEqual(v, val) {
			b.Errorf("retrieved value does not match expected value")
		}
	}

	loopOp(getOp)
}

func loopOp(operation func(key []byte, val []byte)) {
	const ops = 1000000
	for op := 0; op < ops; op++ {
		step := op & 0xff
		key := bigendian.Uint64ToBytes(uint64(step << 48))
		val := bigendian.Uint64ToBytes(uint64(step))
		operation(key, val)
	}
}
