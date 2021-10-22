package aria

import (
	"t_index"
	"t_txn/aria/rd"
	"fmt"

)


type Aria struct {
	// batch_size configure by user
	index *(t_index.Mmap)
	Read_conflict int
	Write_conflict int
}

func (aria *Aria) Reset() {
	aria.index = aria.index.ReNew()
}


func NewAria(mmap_c int) *Aria {
	index := t_index.NewMmap(mmap_c)
	return &(Aria{index, 0, 0})
}


type TXN struct {
	txn_id int
	commited bool
	read_map *(map[string](*rd.Record)) // save read write map for commit validate (one to commit this read/write map must be consistency)
	write_map *(map[string](*rd.Record))
	base *Aria
}


/*
mainly use for internal test
*/
func (t *TXN) GetReadString() string {
	var res string
	for key, r := range (*t.read_map) {
		res = res + fmt.Sprintf("[%v: %v] ", key, r.Get_min_wid())
	}
	return res
}

func (t *TXN) GetWriteString() string {
	var res string
	for key, r := range (*t.write_map) {
		res = res + fmt.Sprintf("[%v: %v] ", key, r.Get_min_wid())
	}
	return res
}

func (t *TXN) Reset() {
	t.read_map = &(map[string](*rd.Record){})
	t.write_map = &(map[string](*rd.Record){})
}	


func (aria *Aria) NewTXN(txn_id int) *TXN {
	r_map := map[string](*rd.Record){}
	w_map := map[string](*rd.Record){}

	return &(TXN{txn_id, false, &r_map, &w_map, aria})
}


func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}


func (t *TXN) IsCommited() bool {
	return t.commited
}

// first phase
func (t *TXN) Write(key string) bool {
	index := t.base.index
	r := quickGetOrInsert(index, key)
	// save this op
	(*(t.write_map))[key] = r
	return r.Write(t.txn_id)
}

func (t *TXN) Read(key string) bool {
	index := t.base.index

	r := quickGetOrInsert(index, key)
	(*(t.read_map))[key] = r
	return r.Read(t.txn_id)
}


/*
exec when all write read return true
if read write return false onece Commit must be false (and no need to do this to validate again)
if read write all return true there need to use Commit to verify it will be abort or not
if commit failed or read/write failed the txn should be exec in next batch with same order
*/
func (t *TXN) Commit() bool { // commit is run in 
	// validate read
	rm := t.read_map
	for _, r := range (*rm) {
		// any less than txn_id measn WAR OR WAW all need to abort
		// if the record do not write by any txn will this validate will ok
		if r.Get_min_wid() < t.txn_id && r.Get_min_wid() != -1 { 
			t.base.Read_conflict = t.base.Read_conflict + 1
			return false
		}
	}
	// validate write
	wm := t.write_map
	for _, r := range (*wm) {
		if r.Get_min_wid() < t.txn_id && r.Get_min_wid() != -1 {
			t.base.Write_conflict = t.base.Write_conflict + 1
			return false
		}
	}
	t.commited = true
	return true
}
