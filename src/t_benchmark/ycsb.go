package t_benchmark

import (
	"strconv"
	"math/rand"
	"t_txn"
)

type Ycsb struct {
	prefix string
	average float64
	variance float64 // for control zkew
	txn_len int
	write_rate float64
}

func NewYcsb(p string, a float64, v float64, l int, w float64) *Ycsb {
	// check the parameter
	return &Ycsb{p, a, v, l, w}
}


/*
this method can be run concurency with one ycsb
*/
func (y Ycsb) NewOPS() *t_txn.OPS {
	ops := make([](t_txn.OP), y.txn_len)
	for i := 0; i < y.txn_len; i++ {
		// generate record
		if rand.Float64() <= y.write_rate {
			ops[i].Is_write = true
		} else {
			ops[i].Is_write = false
		}
		ops[i].Key = y.prefix + strconv.Itoa(int(rand.NormFloat64() * y.variance +  y.average))
	}
	return t_txn.NewOPS(ops)
}
