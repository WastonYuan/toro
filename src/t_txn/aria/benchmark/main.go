package main

import (
	"t_txn/aria"
	"fmt"
)

func main() {
	//YCSB parameter: core skew write_rate txn_len
	fmt.Println("Aria")
	fmt.Printf("TPCC Low Conflict:\nthread\ttps\ttime\twrite_conflict\tread_conflict\n")
	core_l := []int{1, 2, 4, 6, 8, 10, 12, 16}
	for i := 0 ; i < len(core_l); i++ {
		fmt.Printf("%v\t%v\n", core_l[i], aria.YCSB(core_l[i], 0.1, 0.2, 10, 2).GetString())
	}
	fmt.Printf("TPCC High Conflict:\nthread\ttps\ttime\twrite_conflict\tread_conflict\n")
	for i := 0 ; i < len(core_l); i++ {
		fmt.Printf("%v\t%v\n", core_l[i], aria.YCSB(core_l[i], 0.8, 0.5, 100, 2).GetString())
	}
	fmt.Printf("YCSB Write Intensive:\nskew\ttps\ttime\twrite_conflict\tread_conflict\n")
	skew_l := []float64{0.0001, 0.001, 0.01, 0.1, 0.2, 0.4, 0.8, 1}
	for i := 0 ; i < len(skew_l); i++ {
		fmt.Printf("%v\t%v\n", skew_l[i], aria.YCSB(4, skew_l[i], 0.9, 100, 2).GetString())
	}
	fmt.Printf("YCSB Read Intensive:\nskew\ttps\ttime\twrite_conflict\tread_conflict\n")
	for i := 0 ; i < len(skew_l); i++ {
		fmt.Printf("%v\t%v\n", skew_l[i], aria.YCSB(4, skew_l[i], 0.1, 100, 2).GetString())
	}

}

// func TestYCSB(t *testing.T) {YCSB(1, 0.003, 0.7, 10, t)}