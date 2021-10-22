package t_index

import (
	// "fmt"
	"hash/fnv"
	// "t_txn/tpl/rd"
	// "t_txn/calvin/rd"
	"sync"
	"t_log"
)

/* type alias for configure transaction type */
// record is *rd.Record
type record = interface{}


/*
Mmap means multi-map not the memory-map
*/
type Mmap struct { // read only class
	partition int
	map_vec [](*(sync.Map)) // sync map store as map[string](*record)
	mutex_vec [](*(sync.Mutex))
}

func hash(key string) uint32 {
	h := fnv.New32a()
    h.Write([]byte(key))
    return h.Sum32()
}

func (mm Mmap) parition(key string) int {
	return int(hash(key)) % mm.partition
}

func NewMmap(p int) *Mmap {
	v := make([](*(sync.Map)), p)
	v_l := make([](*(sync.Mutex)), p)
	for i:=0; i < p; i++ {
		m := sync.Map{}
		l := sync.Mutex{}
		v[i] = &m
		v_l[i] = &l
	}
	return &Mmap{p, v, v_l}
}

func (mm *Mmap) ReNew() *Mmap {
	p := mm.partition
	return NewMmap(p)
}

/*internal method for test*/
func (mm Mmap) Show() {
	for i:=0 ; i < mm.partition; i++ {
		t_log.Log(t_log.DEBUG,"parition %v\n", i)

		f := func(key, value interface{}) bool {
			t_log.Log(t_log.DEBUG," %v", key)
			return true
		}   
		(*((mm.map_vec)[i])).Range(f)
		// for k, _ := range *((mm.map_vec)[i]) {
		// 	fmt.Printf("%v ", k)
		// }
		t_log.Log(t_log.DEBUG, "\n")
	}
}

func (mm Mmap) Search(key string) record {
	mi := mm.parition(key)
	v, ok := (*(mm.map_vec[mi])).Load(key)
	if ok == false {
		return nil
	} else {
		return v.(record)
	}
} 

/* be ware of using in concurency */
func (mm Mmap) Insert(key string, r record) {
	mi := mm.parition(key)
	(*(mm.map_vec[mi])).Store(key, r)
}

/*
GetOrInsert(key)
if the key is in the index then return
if not then insert to the index and return the get value
all run in atomic
this should be run before Search since it need to lock that may occur conflict

with this the same key will not occur multi dealing record!
*/

func (mm Mmap) GetOrInsert(key string, r record) record {
	mi := mm.parition(key)
	(*(mm.mutex_vec[mi])).Lock()
	defer (*(mm.mutex_vec[mi])).Unlock()
	v, ok := (*(mm.map_vec[mi])).Load(key)
	if ok == false {
		(*(mm.map_vec[mi])).Store(key, r)
		return r
	} else {
		return v.(record)
	}
}

