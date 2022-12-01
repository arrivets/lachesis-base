package flushable

import (
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/batched"
	"github.com/Fantom-foundation/lachesis-base/kvdb/devnulldb"
)

// BackupFlushable is a flushable inmemory database that switches to a
// persistent database when its size reaches a certain limit
type BackupFlushable struct {
	*Flushable
	memorydb  *Flushable
	producer  func() (kvdb.Store, error) // function used to produce the persistent db
	sizeLimit int                        // max size of memorydb
}

func NewBackupFlushable(sizeLimit int, producer func() (kvdb.Store, error), drop func()) *BackupFlushable {
	if producer == nil {
		panic("nil producer")
	}

	memorydb := Wrap(devnulldb.New())

	w := &BackupFlushable{
		Flushable: WrapWithDrop(memorydb, drop),
		memorydb:  memorydb,
		producer:  producer,
		sizeLimit: sizeLimit,
	}

	return w
}

// Flush writes the contents of the write-ahead cache into the underlying db.
// If the underlying db, which is initially an inmemory db, exceeds the size
// limit, its contents are copied to a persistent db, and the memory db is
// cleared.
func (w *BackupFlushable) Flush() (err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.memorydb != nil &&
		w.producer != nil &&
		*w.memorydb.sizeEstimation >= w.sizeLimit {
		fmt.Printf("backup-flushable-db switching. size = %d, limit = %d\n", *w.memorydb.sizeEstimation, w.sizeLimit)
		err = w.switchUnderlying()
		if err != nil {
			return err
		}
	}

	return w.flush()
}

func (w *BackupFlushable) switchUnderlying() error {
	// init the underlying persitent db
	newDB, err := w.producer()
	if err != nil {
		return err
	}

	// copy everything
	wrappedNewDB := batched.Wrap(newDB)
	it := w.memorydb.NewIterator(nil, nil)
	for it.Next() {
		wrappedNewDB.Put(it.Key(), it.Value())
	}
	it.Release()
	wrappedNewDB.Flush()

	// closing the oldDB deletes all its contents because it was never flushed
	w.memorydb.Close()

	// switch
	w.underlying = newDB
	w.flushableReader.underlying = newDB
	w.producer = nil // need once
	w.memorydb = nil

	return nil
}
