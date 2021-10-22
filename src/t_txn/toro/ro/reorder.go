package ro

import (
	"t_txn"
	"sync"
	// "t_log"
)



type Reorder struct {
	// ops_m's key will not be changed
	ops_m *(map[*t_txn.OPS]bool) // if true means being merge to t_star and not be caculated ai again
	t_star_m *(map[string]int)
	rwlock *sync.RWMutex
	alpha int
	beta int
	sorted_list *([](*t_txn.OPS)) // point to ops_m's element
}

func NewReorder(alpha int, beta int) *Reorder {
	ops_m := map[*t_txn.OPS]bool{}
	return &(Reorder{&ops_m, nil, &(sync.RWMutex{}), alpha, beta, nil})
}

func (r *Reorder) GetTstar() *(map[string]int) {
	return r.t_star_m
}


func (r *Reorder) Insert(ops t_txn.OPS) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	(*r.ops_m)[&ops] = false
}


func (r Reorder) GetString() string {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	var res string
	for ops, _ := range *(r.ops_m) {
		res = res + ops.GetString() + "\n"
	}
	return res
}


func (r *Reorder) Init() {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	var ops *t_txn.OPS
	for ops, _ = range *(r.ops_m) {
		// s_ops := ops.Copy()
		// t_log.Log(t_log.DEBUG, "first ops: %v\n", ops.GetString())
		r.t_star_m = ops.WriteMostRightIndexMap()
		break
	}
	(*(r.ops_m))[ops] = true
	sl := make([](*t_txn.OPS), len(*(r.ops_m)))
	sl[0] = ops
	r.sorted_list = &sl
}


func (r *Reorder) AIAfterTstar(c_ops *t_txn.OPS) (int, string) {
	// t_log.Log(t_log.DEBUG, "ai:%v\n", c_ops.GetCache())
	// t_log.Log(t_log.DEBUG, "ai:%v\n", r.t_star.GetCache())
	star_wim := r.t_star_m
	can_rim := c_ops.CacheOrReadMostLeftIndexMap()
	// t_log.Log(t_log.DEBUG, "star: %v\n", star_wim)
	// t_log.Log(t_log.DEBUG, "can: %v\n", can_rim)
	max_dis := 0
	var max_key string
	for key, s_index := range (*star_wim) {
		c_index, ok := (*can_rim)[key] // the key is the intersection of candidate and star
		if ok {
			dis := (s_index - c_index + 1) * r.alpha - r.beta
			if dis > max_dis {
				max_dis = dis
				max_key = key
			}
		}
	}
	return max_dis, max_key
}


func (r *Reorder) TstarMerge(m_ops *t_txn.OPS) {
	// t_star left shift
	for key, s_index := range (*r.t_star_m) {
		(*r.t_star_m)[key] = s_index - r.beta
	}
	m_rim := m_ops.WriteMostRightIndexMap()
	// t_log.Log(t_log.DEBUG, "c_ops wm: %v\n", (*m_rim))
	for key, m_index := range (*m_rim) {
		s_index, ok :=  (*r.t_star_m)[key]
		if ok { // tstar and m_ops both contain this key, choose the larger one
			(*r.t_star_m)[key] = func(a, b int) int {
				if a < b {
					return b
				} else {
					return a 
				}
			}(s_index, m_index)
		} else { // only m_ops contain this key, so add it to tstar
			(*r.t_star_m)[key] = m_index
		}
	}
}


func (r *Reorder) Sort() *([](*t_txn.OPS)) {
	// t_log.Log(t_log.DEBUG, "Sorting: ")
	for i := 1; i < len(*(r.sorted_list)); i++ { // find the next ops
		// t_log.Log(t_log.DEBUG, ".")
		min_ai := -1 // default is -1
		// var dmax_key string
		var selected_ops *t_txn.OPS
		for ops, is_selected := range (*r.ops_m) {
			if is_selected == false {
				var ai int
				ai, _ = r.AIAfterTstar(ops)
				// add the test
				if min_ai == -1 || ai < min_ai { // not selected or less
					min_ai = ai
					selected_ops = ops
					// dmax_key = key
				}
			}
		}
		// t_log.Log(t_log.DEBUG, "tstar: %v\n", (*r.t_star_m))
		// t_log.Log(t_log.DEBUG, "next ops: %v\n", selected_ops.GetString())
		(*(r.sorted_list))[i] = selected_ops
		r.TstarMerge(selected_ops)
		// t_log.Log(t_log.DEBUG, "key: %v dis: %v\n\n", dmax_key, min_ai)
		(*r.ops_m)[selected_ops] = true
	}
	// t_log.Log(t_log.DEBUG, "\n")
	return r.sorted_list
}
