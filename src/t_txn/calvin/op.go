package calvin

import (
	"t_txn"
	"t_index"
	"t_txn/calvin/rd"
	"fmt"
	"sync"
)



type DebugData struct {
	keys *(sync.Map)
}

func NewDebugData() *DebugData {
	return &(DebugData{&(sync.Map{})})
}

type Calvin struct {
	// batch_size configure by user
	index *(t_index.Mmap)
	nt_sam chan int
	debug *DebugData
	is_debug bool
}

func NewCalvin(mmap_c int, nt_c int, is_debug bool) *Calvin {
	index := t_index.NewMmap(mmap_c)
	var dd *DebugData
	if is_debug {
		dd = NewDebugData()
	} else {
		dd = nil
	}
	return &Calvin{index, make(chan int, nt_c), dd, is_debug}
}

type TXN struct {
	txn_id int
	base *Calvin
	keysMap *(map[string]bool) // keys save for validate if coro there is no need keys
}

func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}

func (calvin *Calvin) NewTXN(txn_id int, ops *t_txn.OPS) *TXN {
	// input the lock reserve
	calvin.nt_sam <- 0
	ops.Reset()
	index := calvin.index
	for true {
		op, ok := ops.Next()
		if ok == false {
			break
		}
		key := op.Key
		is_w := op.Is_write
		r := quickGetOrInsert(index, key)
		r.Reserve(txn_id, is_w)
		if calvin.is_debug == true {
			calvin.debug.keys.Store(key, true)
		}
	}
	<- calvin.nt_sam
	km := ops.KeysMap()
	return &TXN{txn_id, calvin, km}
}

func (calvin *Calvin) GetLockString(key string) string {
	var res string
	index := calvin.index
	r := index.Search(key)
	if r == nil {
		res = res + fmt.Sprintf("there is no key %v", key)
		return res
	} else {
		return r.(*rd.Record).LockListString()
	}
}

func (t *TXN) Validate(key string) bool {
	index := t.base.index
	r := index.Search(key)
	if r == nil {
		return false
	} else {
		return r.(*rd.Record).Validate(t.txn_id, (*(t.keysMap))[key])
	}
}

func (t *TXN) GetKeys() map[string]bool {
	return *(t.keysMap)
}

func (t *TXN) ValidateKeys() bool {
	for key, _ := range *(t.keysMap) {
        if t.Validate(key) == false {
			return false
		}
    }
	return true
}

func (t *TXN) Write(key string) bool {
	index := t.base.index
	r := index.Search(key)
	if r == nil {
		return false
	} else {
		return r.(*rd.Record).Write(t.txn_id)
	}
}

func (t *TXN) Read(key string) bool {
	index := t.base.index
	r := index.Search(key)
	if r == nil {
		return false
	} else {
		return r.(*rd.Record).Read(t.txn_id)
	}
}

/*
debug method
*/
func (calvin *Calvin) KeysLockString() string {
	if calvin.is_debug == false {
		return "debug mode not open"
	} else {
		var res string
		km := calvin.debug.keys
		km.Range(func(key ,value interface{}) bool {
			res = res + fmt.Sprintf("key %v ", key)
			res = res + calvin.GetLockString(key.(string))  + "\n"
			return true
		})
		return res
	}
}