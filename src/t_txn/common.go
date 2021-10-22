package t_txn

import(
	"fmt"
	"time"
	"t_util"
	"strconv"
	// "t_log"
)


type Tgorithm interface {
	Run(opss [](*(OPS)), c int, d int) (int, int)
	GetName() string
} 


type OP struct {
	Key string
	Is_write bool
}

// this class is no cucurency
type OPS struct {
	v []OP
	current_index int
	// for reordering cache save the ReadLeftMostMap
	cache interface{} // for cache extra data
}

func (ops *OPS) Copy() *OPS {
	return NewOPS(ops.v)
}


// return bytes
func (ops *OPS) Capacity() int {
	sum := 0
	for i := 0; i < len(ops.v); i++ {
		sum = sum + len((ops.v)[i].Key) + 1 // 1 is the read/write symbol
	}
	return sum
}

func (ops *OPS) CommitSize() int {
	sum := 0
	for i := 0; i < len(ops.v); i++ {
		if (ops.v)[i].Is_write == true {
			sum = sum + len((ops.v)[i].Key)
		}
	}
	return sum

}


/*
NOTICE: use this method the index and cache will reset!
*/
func (ops *OPS) LeftShift(step int) {
	ops.current_index = 0
	ops.cache = nil
	(ops.v) = (ops.v)[step:]
}


func (ops *OPS) SetCache(c interface{}) {
	ops.cache = c
}

func (ops *OPS) GetCache() interface{} {
	return ops.cache
}

func (ops *OPS) Get(index int) OP {
	return ops.v[index]
}

func (ops *OPS) Len() int {
	return len(ops.v)
}

/* all ops will use this except the first ops(direct t_star)) */
func (ops *OPS) CacheOrReadMostLeftIndexMap() *(map[string]int) {
	if ops.cache == nil {
		ops.SetCache(ops.ReadMostLeftIndexMap())
	}
	return ops.cache.(*(map[string]int))
}


func (ops *OPS) ReadMostLeftIndexMap() *(map[string]int) {
	res := map[string]int{}
	for i := 0; i < ops.Len(); i ++ {
		op := ops.Get(i)
		if op.Is_write == false {
			key := op.Key
			_, ok := res[key]
			if !ok { // if ok the index must > res[key] since loop from left to right
				res[key] = i 
			}
		}
	}
	return &res
}


func (ops *OPS) WriteMostRightIndexMap() *(map[string]int) {
	res := map[string]int{}
	for i := 0; i < ops.Len(); i ++ {
		op := ops.Get(i)
		if op.Is_write == true {
			key := op.Key
			res[key] = i
		}
	}
	return &res
}


type TOPS struct {
	Txn_id int
	Ops *OPS
}


func NewTOPS(txn_id int, ops *OPS) *TOPS {
	return &(TOPS{txn_id, ops})
} 


func NewOPS(v []OP) *OPS {
	// t = nil
	return &OPS{v, 0, nil}
}

func (os OPS) GetString() string {
	var res string
	for i := 0 ; i < len(os.v); i++ {
		if os.v[i].Is_write == true {
			res = res + fmt.Sprintf("w:%v", os.v[i].Key)
		} else {
			res = res + fmt.Sprintf("r:%v", os.v[i].Key)
		}
		if i == len(os.v) - 1 {
			// res = res + fmt.Sprintf("\n")
		} else {
			res = res + fmt.Sprintf(" ")
		}
	}
	return res
}


/*
Next return key is_write and ok (return the currenct index and index to next)
*/
func (os *OPS) Next() (OP, bool) {
	c_i := os.current_index
	if c_i == len(os.v) {
		return OP{"", false}, false
	}
	os.current_index = c_i + 1
	return os.v[c_i], true
}

/*
reset the iterator
*/
func (os *OPS) Reset() {
	os.current_index = 0
}

/*
Map is the read write key vs count
*/

func (t OPS) ReadWriteMap(is_write bool) map[string]int {
	r_l := map[string]int{}
	for i:=0; i < len(t.v); i++ {
		if t.v[i].Is_write == is_write {
			key := t.v[i].Key
			value, ok := r_l[key]
			if ok {
				r_l[key] = value + 1
			} else {
				r_l[key] = 1
			}
		}
	}
	return r_l
}

/*
c_rate means the plus write count rate
d_date means the duplicate write count rate
*/
func (t OPS) PossibleWriteMap(c_rate float64, d_rate float64, prefix string)  map[string]int {
	wm := t.ReadWriteMap(true)
	origin_len := len(t.v)
	p_count := int(float64(len(wm)) * c_rate)

	for i := 0; i < p_count; i++ {
		var n_r string
		if t_util.RandFloat() <= d_rate {
			d_i := t_util.RandInt(origin_len)
			n_r = t.v[d_i].Key 
		} else {
			n_r = prefix + strconv.Itoa(t_util.RandInt(p_count))
		}
		value, ok := wm[n_r]
		if ok {
			wm[n_r] = value + 1
		} else {
			wm[n_r] = 1
		}
	}
	return wm
}

func (t OPS) Keys() []string {
	m := map[string]bool{}
	for i:=0; i < len(t.v); i++ {
		key := t.v[i].Key
		m[key] = true
	}
	l := make([]string, len(m))
	i := 0
	for k, _ := range m {
		l[i] = k
		i = i + 1
	}
	return l
}


/*key vs contain write or not*/
func (t OPS) KeysMap() *(map[string]bool) {
	m := map[string]bool{}
	for i:=0; i < len(t.v); i++ {
		key := t.v[i].Key
		is_write := t.v[i].Is_write
		val, ok := m[key]
		if ok {
			if val == false {
				m[key] = is_write
			}
		} else {
			m[key] = is_write
		}
	}
	return &m
}




type Result struct {
	Tps float64
	Runtime time.Duration
	Write_conflict int
	Read_conflict int
}

func (r Result) GetString() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v", r.Tps, r.Runtime, r.Write_conflict, r.Read_conflict)
}



type ROP struct {
	Txn_id int
	Is_write bool
}