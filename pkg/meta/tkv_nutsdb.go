package meta

import (
	"strings"

	"github.com/xujiajun/nutsdb"
)

type nutsdbTxn struct {
	t *nutsdb.Tx
	c *nutsdb.DB
}

func (tx *nutsdbTxn) get(key []byte) []byte {
	bucket := "bucket1"
	item, err := tx.t.Get(bucket, key)

	if err != nil && strings.Contains(err.Error(), nutsdb.ErrKeyNotFound.Error()) {
		return nil
	}
	if err != nil {
		panic(err)
	}

	return item.Value
}

func (tx *nutsdbTxn) gets(keys ...[]byte) [][]byte {
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = tx.get(key)
	}
	return values
}

func (tx *nutsdbTxn) scanRange(begin, end []byte) map[string][]byte {
	bucket := "bucket1"
	var ret = make(map[string][]byte)
	if entries, err := tx.t.RangeScan(bucket, begin, end); err != nil {
		if err == nutsdb.ErrScansNoResult {
			return ret
		}
	} else {
		for _, entry := range entries {
			ret[string(entry.Key)] = entry.Value
		}
	}
	return ret
}

func (tx *nutsdbTxn) scanKeys(prefix []byte) [][]byte {
	bucket := "bucket1"
	var ret [][]byte
	if entries, _, err := tx.t.PrefixScan(bucket, prefix, 0, 1024); err != nil {
		if err == nutsdb.ErrPrefixSearchScansNoResult {
			return ret
		}
	} else {
		for _, entry := range entries {
			ret = append(ret, entry.Key)
		}
	}
	return ret
}

func (tx *nutsdbTxn) scanValues(prefix []byte, limit int, filter func(k, v []byte) bool) map[string][]byte {
	if limit == 0 {
		return nil
	}
	bucket := "bucket1"
	var ret = make(map[string][]byte)
	if entries, _, err := tx.t.PrefixScan(bucket, prefix, 0, 1024); err != nil {
		if err == nutsdb.ErrPrefixSearchScansNoResult {
			return ret
		}
	} else {
		for _, entry := range entries {
			if filter == nil || filter(entry.Key, entry.Value) {
				ret[string(entry.Key)] = entry.Value
				if limit > 0 {
					if limit--; limit == 0 {
						break
					}
				}
			}
		}
	}
	return ret
}

func (tx *nutsdbTxn) exist(prefix []byte) bool {
	if len(prefix) == 0 {
		return false
	}
	bucket := "bucket1"
	if _, _, err := tx.t.PrefixScan(bucket, prefix, 0, 1); err != nil {
		return false
	}
	return true
}

func (tx *nutsdbTxn) set(key, value []byte) {
	bucket := "bucket1"
	err := tx.t.Put(bucket, key, value, 0)
	if err != nil {
		panic(err)
	}
}

func (tx *nutsdbTxn) append(key []byte, value []byte) []byte {
	list := append(tx.get(key), value...)
	tx.set(key, list)
	return list
}

func (tx *nutsdbTxn) incrBy(key []byte, value int64) int64 {
	buf := tx.get(key)
	newCounter := parseCounter(buf)
	if value != 0 {
		newCounter += value
		tx.set(key, packCounter(newCounter))
	}
	return newCounter
}

func (tx *nutsdbTxn) dels(keys ...[]byte) {
	bucket := "bucket1"
	for _, key := range keys {
		if err := tx.t.Delete(bucket, key); err != nil {
			panic(err)
		}
	}
}

type nutsdbClient struct {
	client *nutsdb.DB
}

func (c *nutsdbClient) name() string {
	return "nutsdb"
}

func (c *nutsdbClient) shouldRetry(err error) bool {
	return err == nutsdb.ErrTxClosed
}

func (c *nutsdbClient) txn(f func(kvTxn) error) (err error) {
	tx, err := c.client.Begin(true)
	if err != nil {
		return err
	}

	if err = f(&nutsdbTxn{tx, c.client}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (c *nutsdbClient) scan(prefix []byte, handler func(key []byte, value []byte)) error {
	bucket := "bucket1"
	if err := c.client.View(
		func(tx *nutsdb.Tx) error {
			if entries, _, err := tx.PrefixScan(bucket, prefix, 0, 102400); err != nil {
				return err
			} else {
				for _, entry := range entries {
					handler(entry.Key, entry.Value)
				}
			}
			return nil
		}); err != nil {
		return err
	}
	return nil
}
func (c *nutsdbClient) reset(prefix []byte) error {
	bucket := "bucket1"
	err := c.client.Update(
		func(tx *nutsdb.Tx) error {
			entries, err := tx.GetAll(bucket)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if err := tx.Delete(bucket, entry.Key); err != nil {
					return err
				}
			}
			return nil
		})
	return err
}

func (c *nutsdbClient) close() error {
	return c.client.Close()
}

func newNutsdbClient(addr string) (tkvClient, error) {
	bucket := "bucket1"
	opt := nutsdb.DefaultOptions
	opt.Dir = addr
	opt.SyncEnable = false
	logger.Infof("addr: %s", addr)
	client, err := nutsdb.Open(opt)
	if err != nil {
		return nil, err
	}
	err = client.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key")
			value := []byte("value")
			return tx.Put(bucket, key, value, 0)
		})
	return &nutsdbClient{client}, err
}

func init() {
	Register("nutsdb", newKVMeta)
	drivers["nutsdb"] = newNutsdbClient
}
