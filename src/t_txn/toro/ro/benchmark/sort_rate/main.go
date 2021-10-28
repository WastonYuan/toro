package main

import (
	"t_benchmark"
	// "t_util"
	// "sync"
	"fmt"
	// "runtime"
	"t_log"
	"time"
	"t_txn"
	"t_txn/toro/ro"
	// "t_txn/toro/rd"
	"t_txn/calvin"
	"t_txn/bohm"
	"t_txn/pwv"
	"t_txn/toro"
)

const t_count = 500

func main() {

	ro_rate := []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}
	fmt.Println("ro_rate(%)\tCalvin\tBOHM\tPWV\tToro")
	/* generate txn and reorder(or not) */

	ycsb := t_benchmark.NewYcsb("r", 100000, 50, 100, 0.5)
	opss := make([](*(t_txn.OPS)), t_count)
	for i := 0; i < t_count; i++ {

		ops := ycsb.NewOPS() // actually read write sequence
		opss[i] = ops
	}
	
	for i := 0 ; i < len(ro_rate); i++ {
		c, b, p, t := YCSB_RoRate(4, 4, int(ro_rate[i]*t_count), opss, ro_rate[i])
		fmt.Printf("%v\t%v\t%v\t%v\t%v\n", ro_rate[i], c*100, b*100, p*100, t*100)
	}
}

/*
Return Increase Rate:
Calvin, BOHM, PWV, Toro
*/

func YCSB_RoRate(c int, d int, k int, opss [](*(t_txn.OPS)), sort_rate float64) (float64, float64, float64, float64)  {
	// t_log.Loglevel = t_log.DEBUG
	t_log.Loglevel = t_log.ERROR
	
	sort_count := int(t_count * sort_rate)
	ro_opss := make([](*(t_txn.OPS)), t_count)
	
	
	// for test hang
	// degree := make(chan int, 3)
	

	reorder := ro.NewReorder(1, 2)
	for i := 0; i < sort_count; i++ {

		ops := opss[i] // actually read write sequence
		reorder.Insert(*ops)
	}
	reorder.Init()
	ro_opss = *(reorder.Sort(k))

	// Run at this
	ro_opss = append(ro_opss, opss[sort_count:]...)


	var tgo_l  = []t_txn.Tgorithm{calvin.CalvinShell{}, bohm.BohmShell{}, pwv.PWVShell{}, toro.ToroShell{}}

	res := make([]float64, len(tgo_l))


	for i := 0; i < len(tgo_l); i ++ { // print performance
		start := time.Now()
		tgo_l[i].Run(opss, c, d)
		elapsed := time.Since(start)
		tps := float64(t_count)/(elapsed.Seconds())
		start = time.Now()
		tgo_l[i].Run(ro_opss, c, d)
		elapsed = time.Since(start)
		tps_ro := float64(t_count)/(elapsed.Seconds())
		res[i] = (tps_ro - tps) / tps
	}
	return res[0], res[1], res[2], res[3]
}