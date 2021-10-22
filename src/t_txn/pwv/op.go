package pwv

import (
	"t_index"
	"t_txn/pwv/rd"
	"t_log"
	"sync"
)


type DEBUG struct {
	keys *sync.Map
}

type PWV struct {
	// batch_size configure by user
	index *(t_index.Mmap)
	debug *DEBUG 
	Read_conflict int	
}

func NewPWV(mmap_c int, is_debug bool) *PWV {
	index := t_index.NewMmap(mmap_c)
	var debug *DEBUG = nil
	if is_debug {
		debug = &(DEBUG{&(sync.Map{})})
	}
	return &(PWV{index, debug, 0})
}

type TXN struct {
	txn_id int
	write_map *(map[string]int) // pwv should know the write set in start
	write_version *(map[*(rd.Version)]bool) // true means being writed which will clean when revert
	o_read_version *(map[*(rd.Version)]bool) // for pwv read the modified version and waiting for it staged and will clean when revert
	base *PWV
}

func (pwv *PWV) NewTXN(txn_id int, write_map map[string]int) *TXN {
	if pwv.debug != nil {
		for key, _ := range write_map {
			// (*(pwv.debug.keys))[key] = true
			(*(pwv.debug.keys)).Store(key, true)
		}
	}
	return &(TXN{txn_id, &write_map, &(map[*(rd.Version)]bool{}), &(map[*(rd.Version)]bool{}) ,pwv})
}

func (pwv *PWV) GetWriteConflict() int {
	return pwv.Read_conflict
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
	r.Install(t.txn_id, rd.PENDING)
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

func (pwv *PWV) KeysVersionString() string {
	keys_map := pwv.debug.keys
	index := pwv.index
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
	for version, _ := range (*wv) {
		if version.GetStats() == rd.MODIFIED {
			version.UpdateStats(rd.STAGED)
		} else {
			t_log.Log(t_log.ERROR, "Staged failed Version: %v", version.GetString())
			return false
		}
	}
	return true
}


/*
logically this will alway return true
*/
func (t *TXN) Revert() bool {
	wv := t.write_version
	for version, written := range (*wv) {
		if written == true {
			if version.GetStats() == rd.MODIFIED {
				version.UpdateStats(rd.PENDING)
			} else {
				t_log.Log(t_log.ERROR, "Revert error with not a pending stats: %v", version.GetString())
				return false
			}
			(*wv)[version] = false
		}
	}
	// t.write_version = &(map[*(rd.Version)]bool{})
	t.base.Read_conflict = t.base.Read_conflict + 1
	return true
}

/*
Toro also can improve in this place
*/
func (t *TXN) CheckOtherTXNStaged() bool {
	orv := t.o_read_version
	for version, _ := range (*orv) {
		if version.GetStats() < rd.STAGED {
			// t_log.Log(t_log.DEBUG, "txn %v hang: %v\n", t.txn_id, version.GetString())
			return false
		}
	}
	return true
}