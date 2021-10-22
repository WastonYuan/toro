package main


import (
	"t_log"
	"t_benchmark"
	"t_txn"
	"time"
	"t_txn/toro"
	"fmt"
)

func main() {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.ERROR
	s := 0.01
	l := 100
	w := 0.5
	v := 1 / s
	c := 4
	d := 4
	ycsb := t_benchmark.NewYcsb("r", 100000, v, l, w)
	const t_count = 100
	opss := make([](*(t_txn.OPS)), t_count)
	for i := 0; i < t_count; i++ {

		ops := ycsb.NewOPS() // actually read write sequence
		opss[i] = ops
	}
	
	
	// Run at this
	for pc := 0; pc < 10; pc ++ {
		for pd := 0; pd < 10; pd ++ {
			start := time.Now()
			toro.ToroShell{}.RunCore(opss, c, d, float64(pc), float64(pd))
			elapsed := time.Since(start)
			fmt.Printf("%v\t", float64(t_count)/(elapsed.Seconds()))
		}
		fmt.Println()
	}
	

}