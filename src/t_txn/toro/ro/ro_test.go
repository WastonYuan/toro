package ro

import (
	"testing"
	"t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"t_log"
	// "time"
	"t_txn"
)


/*
go test t_txn/toro/ro -v -run TestBasic
*/

func TestBasic(t *testing.T) {

	// a := interface{}(nil)

	// fmt.Println(a)

	t_log.Loglevel = t_log.DEBUG
	ycsb := t_benchmark.NewYcsb("r", 100, 1, 10, 0.3)
	var wg sync.WaitGroup
	const txn_count = 10
	opss := [txn_count](*t_txn.OPS){}
	ro := NewReorder(1, 1)
	for i := 0; i < txn_count; i++ {
		wg.Add(1)
		go func(txn_id int) {

			ops := ycsb.NewOPS()
			opss[txn_id] = ops
			ro.Insert(*ops)
			wg.Done()
		}(i)
	}
	wg.Wait()
	ro.Init()
	res := ro.Sort()
	// t_log.Log(t_log.DEBUG, "%v\n", (*res))
	for i := 0; i < len(*res); i++ {
		t_log.Log(t_log.DEBUG, "%v: %v\n", i, (*res)[i].GetString())
	}
	// t_log.Log(t_log.DEBUG, "tstar:%v ops:%v dis:%v key:%v\n", ro.GetTstar(), opss[7].GetString(), dis, key)
	
}