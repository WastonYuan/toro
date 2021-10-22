package calvin

import (
	"testing"
	"t_benchmark"
	"t_util"
	"sync"
	"fmt"
	"t_log"
	"time"
	// "t_txn"
)

/*
go test t_txn/calvin -v -run TestYCSB
*/

func YCSB_t(c int, s float64, w float64 ,l int, t *testing.T) {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.PANIC
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("r", 100, v, l, w)
	calvin := NewCalvin(2, 1, true)
	core := make(chan int, c)
	t_count := 100

	var first_phase sync.WaitGroup 
	var second_phase sync.WaitGroup 

	first_phase.Add(t_count)
	second_phase.Add(t_count)

	// for test hang

	start := time.Now()
	for i:=0; i < t_count; i++ {
		go func(txn_id int) {
			ops := ycsb.NewOPS()
			t_log.Log(t_log.DEBUG, "txn %v ops %v\n", txn_id, ops.GetString())
			// first phase reserve
			core <- 0 // add lock part must can not has print!!!
			txn := calvin.NewTXN(txn_id, ops)
			<- core
			// fmt.Println(txn)
			t_log.Log(t_log.DEBUG, "txn %v first phase finished\n", txn.txn_id)
			first_phase.Done()
			
			first_phase.Wait()
			// second phase validate and exec
			for true {
				core <- 0
				vr := txn.ValidateKeys()
				<- core
				if vr {
					// exec
					ops.Reset()
					for true {
						op, ok := ops.Next()
						if ok == false {
							ops.Reset()
							break
						}
						key := op.Key
						if op.Is_write {
							core <- 0
							if txn.Write(key) == false {
								t_log.Log(t_log.ERROR, "txn %v write key %v error\n", txn.txn_id, key)
								t_log.Log(t_log.ERROR, "lock list: %v\n", calvin.KeysLockString())
							}
							<- core
						} else {
							core <- 0
							if txn.Read(key) == false {
								t_log.Log(t_log.ERROR, "txn %v read key %v error\n", txn.txn_id, key)
								t_log.Log(t_log.ERROR, "lock list: %v\n", calvin.KeysLockString())
							}
							<- core
						}
					}
					t_log.Log(t_log.DEBUG, "txn %v finished\n", txn.txn_id)
					second_phase.Done()
				}
				
			}
		}(i)
	}

	// go func() {
	// 	for true {
	// 		time.Sleep(5 * time.Second)
	// 		t_log.Log(t_log.DEBUG, "%v", calvin.KeysLockString())
	// 		t_log.Log(t_log.DEBUG, "\n\n\n")
	// 	}
	// }()

	second_phase.Wait()
	elapsed := time.Since(start)
	fmt.Printf("WriteConflict: %v, ReadConflict: %v, TakeTime: %v, OPS = %v\n", t_util.RWLock_WrtieConflict, t_util.RWLock_ReadConflict, elapsed, float64(t_count)/(elapsed.Seconds()))
}


/*
parameter:
core, skew, write_rate, len

*/
func TestYCSB(t *testing.T) {YCSB_t(1, 0.003, 0.7, 10, t)}