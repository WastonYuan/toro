package toro

import (
	"t_index"
	"t_txn/toro/rd"
	"t_log"
	"sync"
	"fmt"
)


type DEBUG struct {
	keys *sync.Map
}

type Toro struct {
	// batch_size configure by user
	index *(t_index.Mmap)
	debug *DEBUG 
	Read_conflict int	
}

func NewToro(mmap_c int, is_debug bool) *Toro {
	index := t_index.NewMmap(mmap_c)
	var debug *DEBUG = nil
	if is_debug {
		debug = &(DEBUG{&(sync.Map{})})
	}
	return &(Toro{index, debug, 0})
}

type TXN struct {
	txn_id int
	write_map *(map[string]int) // toro should know the write set in start
	write_version *(map[*(rd.Version)]bool) // true means being writed false means installed but not write (which should be abort!)
	o_read_version *(map[*(rd.Version)]bool) // for toro read the modified version and waiting for it staged and will clean when revert
	base *Toro
}


func (t *TXN) GetReserveWriteString() string {
	var res string
	wm := t.write_map
	for key, c := range (*wm) {
		res = res + fmt.Sprintf("[%v: %v] ", key, c)
	}
	return res
}

func (toro *Toro) NewTXN(txn_id int, write_map map[string]int) *TXN {
	if toro.debug != nil {
		for key, _ := range write_map {
			// (*(toro.debug.keys))[key] = true
			(*(toro.debug.keys)).Store(key, true)
		}
	}
	return &(TXN{txn_id, &write_map, &(map[*(rd.Version)]bool{}), &(map[*(rd.Version)]bool{}) ,toro})
}

func (toro *Toro) GetWriteConflict() int {
	return toro.Read_conflict
}


func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}

/*
first phase write (for use instead of InstallKeys)
*/
func (t *TXN) Install(key string)  {
	index := t.base.index
	// find or insert the key to index
	r := quickGetOrInsert(index, key)
	// r.install
	nv := r.Install(t.txn_id, rd.PENDING)
	(*t.write_version)[nv] = false
}

/*
Install all keys in the first phase
*/
func (t *TXN) InstallKeys() {
	wm := t.write_map
	for key, count := range (*wm) {
		for i := 0; i < count; i ++ {
			t.Install(key)
		}
	}
}

func (toro *Toro) KeysVersionString() string {
	keys_map := toro.debug.keys
	index := toro.index
	var res string
	// km.Range(func(key ,value interface{}) bool {
	// 	res = res + fmt.Sprintf("key %v ", key)
	// 	res = res + calvin.GetLockString(key.(string))  + "\n"
	// 	return true
	// })
	keys_map.Range(func(k ,v interface{}) bool {
		key := k.(string)
		r := index.Search(key).(*(rd.Record))
		res = res + key + ": "
		if r == nil {
			t_log.Log(t_log.ERROR, "key %v search failed\n", key)
			// return false
		} else {
			res = res + r.VersionListString()
		}
		res = res + "\n"
		return true
	})
	return res
}


func (t *TXN) Write(key string) bool {
	index := t.base.index
	wv := t.write_version
	r := index.Search(key)
	if r == nil { // impossible run to this
		return false
	}
	v := r.(*(rd.Record)).Write(t.txn_id)
	if v == nil {
		return false
	} else {
		(*wv)[v] = true // save the writing version for revert
		return true
	}
}


func (t *TXN) Read(key string) bool {
	index := t.base.index
	orv := t.o_read_version
	// r := index.Search(key)
	// if r == nil {
	// 	t_log.Log(t_log.INFO, "no record %v in index\n", key)
	// 	return false
	// }
	r := quickGetOrInsert(index, key)
	v, is_n := r.Read(t.txn_id)
	if is_n {
		v = r.Install(-1, rd.COMMITED)
	}
	if v == nil {
		// t_log.Log(t_log.DEBUG, "txn %v read key %v failed in vl %v\n", t.txn_id, key, r.VersionListString())
		t.base.Read_conflict = t.base.Read_conflict + 1
		return false
	} else {
		if v.GetTXN() != t.txn_id {
			(*orv)[v] = true
		}
		return true
	}
}

/*
logically it will always return true
*/
func (t *TXN) Staged() bool {
	wv := t.write_version
	for version, written := range (*wv) {
		if version.GetStats() == rd.MODIFIED && written == true {
			version.UpdateStats(rd.STAGED)
		} else if written == false {
			version.UpdateStats(rd.ABORT)
			// t_log.Log(t_log.DEBUG, "txn %v add aborted: %v", t.txn_id , version.GetString())
		} else {
			t_log.Log(t_log.ERROR, "Staged failed Version: %v", version.GetString())
			return false
		}
	}
	// abort the not write pending install version

	return true
}


/*
logically this will alway return true
*/
func (t *TXN) Revert() bool {
	// if revert there is not aborted stats for now
	wv := t.write_version
	for version, written := range (*wv) {
		if written == true {
			if version.GetStats() == rd.MODIFIED {
				version.UpdateStats(rd.PENDING)
			} else {
				t_log.Log(t_log.ERROR, "Revert error with not a modified stats: %v\n", version.GetString())
				return false
			}
			(*wv)[version] = false // change to false (not be write yet but next write record is the same)
		}
	}
	// wv = &(map[*(rd.Version)]bool{})
	t.base.Read_conflict = t.base.Read_conflict + 1
	return true
}

/*
Toro also can improve in this place
*/
func (t *TXN) CheckOtherTXNStaged() bool {
	orv := t.o_read_version
	for version, _ := range (*orv) {
		if version.GetStats() < rd.STAGED { // ABORT PENDING OR MODIFIED 
			// t_log.Log(t_log.DEBUG, "txn %v hang: %v\n", t.txn_id, version.GetString())
			t.o_read_version = &(map[*(rd.Version)]bool{})
			return false
		}
	}
	return true
}