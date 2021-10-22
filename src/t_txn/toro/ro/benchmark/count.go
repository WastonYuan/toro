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
	for i := 0 ; i < len(skew_l); i++ {
		fmt.Printf("skew :%v\n", skew_l[i])
		YCSB(4, skew_l[i], 0.9, 100, 2)
	}
}

/*
Return Increase Rate:
Calvin, 
*/

func YCSB(c int, s float64, w float64 ,l int, d int) {
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

	var ro_l = []bool{false, true}

	var opss_ptr *([](*(t_txn.OPS)))
	for j := 0; j < len(ro_l); j ++ {
		ro := ro_l[j]
		for i := 0; i < len(tgo_l); i ++ { // print title
			if ro == true {
				opss_ptr = &ro_opss
				fmt.Printf("%v_ro\t", tgo_l[i].GetName())
			} else {
				opss_ptr = &opss
				fmt.Printf("%v\t", tgo_l[i].GetName())
			}
		}
		fmt.Printf("\n")
		for i := 0; i < len(tgo_l); i ++ { // print performance
			start := time.Now()
			tgo_l[i].Run(*opss_ptr, c, d)
			elapsed := time.Since(start)
			fmt.Printf("%v\t", float64(t_count)/(elapsed.Seconds()))
		}
		fmt.Printf("\n")
	}
	fmt.Printf("===============================================\n")
	
}