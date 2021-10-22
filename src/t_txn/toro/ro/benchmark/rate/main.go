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

func main() {

	skew_l := []float64{0.0001, 0.001, 0.01, 0.1, 0.2, 0.4, 0.8, 1}
	fmt.Println("skew\tCalvin\tBOHM\tPWV\tToro")
	for i := 0 ; i < len(skew_l); i++ {
		c, b, p, t := YCSB_RoRate(4, skew_l[i], 0.5, 100, 4)
		fmt.Printf("%v\t%v\t%v\t%v\t%v\n", skew_l[i], c*100, b*100, p*100, t*100)
	}
}

/*
Return Increase Rate:
Calvin, BOHM, PWV, Toro
*/

func YCSB_RoRate(c int, s float64, w float64 ,l int, d int) (float64, float64, float64, float64)  {
	// t_log.Loglevel = t_log.DEBUG
	t_log.Loglevel = t_log.ERROR
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("r", 100000, v, l, w)
	
	const t_count = 100
	opss := make([](*(t_txn.OPS)), t_count)
	ro_opss := make([](*(t_txn.OPS)), t_count)
	
	
	// for test hang
	// degree := make(chan int, 3)
	

	/* generate txn and reorder(or not) */
	for i := 0; i < t_count; i++ {

		ops := ycsb.NewOPS() // actually read write sequence
		opss[i] = ops
	}
	reorder := ro.NewReorder(1, 2)
	for i := 0; i < t_count; i++ {

		ops := opss[i] // actually read write sequence
		reorder.Insert(*ops)
	}
	reorder.Init()
	ro_opss = *(reorder.Sort())

	// Run at this


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