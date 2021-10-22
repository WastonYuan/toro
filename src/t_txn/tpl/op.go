package tpl

import (
	"t_util"
	"sort"
	"t_index"
	"t_txn/tpl/rd"
	"t_txn"
	// "fmt"
	"t_log"
)


type TPL struct {
	index *(t_index.Mmap)
	rw_sam chan int // read write concurency count
	rw_dur *t_util.Period
}

func NewTPL(mmap_c, rw_sam_len int, rw_wait_e, rw_wait_v float64) *TPL {
	index := t_index.NewMmap(mmap_c)
	sam := make(chan int, rw_sam_len) // most read write concurrency
	period := t_util.NewPeriod(rw_wait_e, rw_wait_v)
	return &TPL{index, sam, period}
}


type TXN struct {
	txn_id int
	read_map map[string]int
	write_map map[string]int
	/* below this state */
	lock_list [](t_txn.OP)
	lock_index int // the index point lock is under lock!
	base *TPL
}

func (t TXN) PrintLockList() {
	t_log.Log(t_log.DEBUG,"txn %v LockList %v\n", t.txn_id, t.lock_list)
}

func less(a t_txn.OP, b t_txn.OP) bool {
	if a.Key < b.Key {
		return true
	} else if a.Key == b.Key && a.Is_write == true && b.Is_write == false {
		return true
	} else {
		return false
	}

}

func lessOrEqual(a t_txn.OP, b t_txn.OP) bool {
	if a.Key < b.Key {
		return true
	} else if a.Key == b.Key && ( a.Is_write == true || ( a.Is_write == false &&  b.Is_write == false ) ) {
		return true
	} else {
		return false
	}

}

// 2pl need to know read write list before exec
func (tpl *TPL) NewTXN(tid int, r_m map[string]int, w_m map[string]int) *TXN {
	// make the lock list
	// fmt.Println(tid, r_m, w_m)
	ll := make([](t_txn.OP), 0)
	for k := range r_m {
		ll = append(ll, t_txn.OP{k, false})
	}
	for k := range w_m {
		ll = append(ll, t_txn.OP{k, true})
	}

	sort.Slice(ll, func(i, j int) bool {
		return less(ll[i], ll[j])
	  })

	return &TXN{tid, r_m, w_m, ll, -1, tpl}
}


func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}


func (txn *TXN) lock_smaller_key(index *(t_index.Mmap), op t_txn.OP) {

	for i := txn.lock_index + 1; i < len(txn.lock_list); i++ {
		if i == len(txn.lock_list) {
			break
		}
		l_op := txn.lock_list[i]
		if lessOrEqual(l_op, op) {
			// get or new the record
			r := quickGetOrInsert(index, l_op.Key)
			// add lock
			if l_op.Is_write == true {
				// fmt.Printf("txn %v add %v lock begin lock info %v\n", txn.txn_id, l_op.Key, r.LockInfo())
				r.SyncLock(txn.txn_id)
				// fmt.Printf("txn %v add %v lock done lock info %v\n", txn.txn_id, l_op.Key, r.LockInfo())
			} else {
				// fmt.Printf("txn %v add %v rlock begin lock info %v\n", txn.txn_id, l_op.Key, r.LockInfo())
				r.SyncRLock(txn.txn_id)
				// fmt.Printf("txn %v add %v rlock done lock info %v\n", txn.txn_id, l_op.Key, r.LockInfo())
			}
			// update lock index
			txn.lock_index = i
		} else {
			break
		}
	}
}

func (txn *TXN) SyncWrite(key string) {
	index := txn.base.index
	samphore := txn.base.rw_sam
	period := txn.base.rw_dur
	// 1. get the smaller record and add lock. (if lock the rwlock will not lock again but just warning)
	txn.lock_smaller_key(index, t_txn.OP{key, true})

	// 2. check if need to release then release or not
	w_c := txn.write_map[key] - 1
	txn.write_map[key] = w_c
	is_relase := false
	if w_c == 0 {
		is_relase = true
	}
	// 3. write to the record
	r := quickGetOrInsert(index, key)
	// fmt.Printf("txn %v lock ok and write %v begin is_relase is %v and lock info:%v\n", txn.txn_id, key, is_relase, r.LockInfo())
	r.SyncWrite(samphore, txn.txn_id, *period, is_relase)
	// fmt.Printf("txn %v lock ok and write %v done is_relase is %v and lock info:%v\n", txn.txn_id, key, is_relase, r.LockInfo())

}

func (txn *TXN) SyncRead(key string) {
	index := txn.base.index
	samphore := txn.base.rw_sam
	period := txn.base.rw_dur
	// 1. get the smaller record and add lock. (if lock the rwlock will not lock again but just warning)
	txn.lock_smaller_key(index, t_txn.OP{key, false})

	// 2. check if need to release then release or not
	w_c := txn.read_map[key] - 1
	txn.read_map[key] = w_c
	is_relase := false
	if w_c == 0 {
		is_relase = true
	}
	// 3. write to the record
	r := quickGetOrInsert(index, key)
	// fmt.Printf("txn %v lock ok and read %v begin is_relase is %v and lock info:%v\n", txn.txn_id, key, is_relase, r.LockInfo())
	r.SyncRead(samphore, txn.txn_id, *period, is_relase)
	// fmt.Printf("txn %v lock ok and read %v done is_relase is %v and lock info:%v\n", txn.txn_id, key, is_relase, r.LockInfo())
}

