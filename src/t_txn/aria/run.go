package aria

import (
	"t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"t_log"
	"time"
	"t_txn"
	"container/list"
)

/*
go test t_txn/calvin -v -run TestYCSB
*/

/*
d this the second phase parallel count
which control the conflict and concurency degree (the more concurency the more conflict)
*/
func YCSB(c int, s float64, w float64 ,l int, d int) t_txn.Result {
	// t_log.Loglevel = t_log.DEBUG
	t_log.Loglevel = t_log.ERROR
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("r", 100000, v, l, w)


	const t_count = 1000
	opss := make([](*(t_txn.OPS)), t_count)

	

	// build txns
	for i:=0; i < t_count; i++ {
		ops := ycsb.NewOPS()
		opss[i] = ops
	}

	start := time.Now()

	wc, rc := AriaShell{}.Run(opss, c, d)
	
	/* Run until all txn commit */
	elapsed := time.Since(start)
	return t_txn.Result{float64(t_count)/(elapsed.Seconds()), elapsed, wc, rc}
}



type AriaShell struct {}

func (t AriaShell) GetName() string {
	return "Aria"
}

func (t AriaShell) Run(opss [](*(t_txn.OPS)), c int, d int) (int, int) {
	aria := NewAria(2)
	core := make(chan int, c)
	t_count := len(opss)

	tops := map[*TXN](*(t_txn.OPS)){} // save *TOPS
	// degree := make(chan int, d)
	// build txns
	for i:=0; i < t_count; i++ {
		ops := opss[i]
		txn := aria.NewTXN(i)
		tops[txn] = ops
	}


	// for test hang
	// degree := make(chan int, 3)
	var batch sync.WaitGroup
	for pi := 0; true; pi++ { //  run until all txn is commited (topl is empty), pi just for debug

		commited_txns := list.New()
		t_log.Log(t_log.DEBUG, "tops len %v\n", len(tops))
		// Execution phase
		for txn, ops := range tops { // loop for each txn
			batch.Add(1)
			
			// t_log.Log(t_log.DEBUG, "txn %v begin %v\n", txn.txn_id, ops.GetString())
			go func(txn *TXN, ops *(t_txn.OPS)) {
				
				defer batch.Done()
				ops.Reset()
				core <- 0
				for true {
					op, ok := ops.Next()
					if ok == false { // txn finished
						ops.Reset()
						// t_log.Log(t_log.DEBUG, "txn %v may be ok\n", txn.txn_id)
						break
					}
					key := op.Key
					is_write := op.Is_write
 
					if is_write {
						if txn.Write(key) == false {
							// t_log.Log(t_log.DEBUG, "txn %v write %v abort\n", txn.txn_id, key)
							break
						} else {
							// t_log.Log(t_log.DEBUG, "txn %v write %v ok\n", txn.txn_id, key)
						}
					} else {
						if txn.Read(key) == false {
							// t_log.Log(t_log.DEBUG, "txn %v read %v abort\n", txn.txn_id, key)
							break
						} else {
							// t_log.Log(t_log.DEBUG, "txn %v read %v ok\n", txn.txn_id, key)
						}
					}
				}
				<- core
				// t_log.Log(t_log.DEBUG, "txn %v finished in this phase\n", txn.txn_id)
			}(txn, ops)
			
		
		}
		// time.Sleep(5 * time.Second)
		// t_log.Log(t_log.DEBUG, "============ phase %v ok ============\n", pi)
		batch.Wait()
		// Commit phase (this do not need to parallel if so the next delete part should loop all txn again)
		for txn, _ := range tops {
			if txn.Commit() {// it is not must success 
				commited_txns.PushBack(txn)
				// t_log.Log(t_log.DEBUG, "txn %v done\n", txn.txn_id)
			}
			// t_log.Log(t_log.DEBUG, "txn %v rm: %v, wm:%v\n", txn.txn_id, txn.GetReadString(), txn.GetWriteString())
		}
		// delete the commited txn in topsl
		for e := commited_txns.Front(); e != nil; e = e.Next() {
			txn := e.Value.(*TXN)
			delete(tops, txn)
		}
		if len(tops) == 0 { // all txn is commited
			break
		}
		aria.Reset() // clean the index
	}

	
	// the read write conflict only add up in commit
	return aria.Write_conflict, aria.Read_conflict
	


}

/*
parameter:
core, skew, write_rate, len

*/
// func TestYCSB(t *testing.T) {YCSB(1, 0.003, 0.7, 10, t)}