package memstore

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
)

func BenchmarkMemstore(b *testing.B) {
	memstore := New()

	const recs = 64

	for _, flushPeriod := range []int{0, 1, 10, 100, 1000} {
		for goroutines := 1; goroutines <= recs; goroutines *= 2 {
			for reading := 0; reading <= 1; reading++ {
				name := fmt.Sprintf(
					"%d goroutines with flush every %d ops, readingExtensive=%d",
					goroutines, flushPeriod, reading)
				b.Run(name, func(b *testing.B) {
					benchmarkMemstore(memstore, goroutines, b.N, flushPeriod, reading != 0)
				})
			}
		}
	}
}

func benchmarkMemstore(db *Database, goroutines, recs, flushPeriod int, readingExtensive bool) {
	leftRecs := recs
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		var ops = recs/goroutines + 1
		if ops > leftRecs {
			ops = leftRecs
		}
		leftRecs -= ops
		go func(i int) {
			defer wg.Done()

			for op := 0; op < ops; op++ {
				step := op & 0xff
				key := bigendian.Uint64ToBytes(uint64(step << 48))
				val := bigendian.Uint64ToBytes(uint64(step))

				rare := time.Now().Unix()%100 == 0
				if readingExtensive == rare {
					err := db.Put(key, val)
					if err != nil {
						panic(err)
					}
				} else {
					got, err := db.Get(key)
					if err != nil {
						panic(err)
					}
					if got != nil && !bytes.Equal(val, got) {
						panic("invalid value")
					}
				}
			}
		}(i)
	}
	wg.Wait()
}
