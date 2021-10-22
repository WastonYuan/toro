package toro

import (
	"t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"runtime"
	"t_log"
	"time"
	"t_txn"
	"t_txn/toro/ro"
	// "t_txn/toro/rd"
)

/*
go test t_txn/calvin -v -run TestYCSB
*/

/*
d this the second phase parallel count
which control the conflict and concurency degree (the more concurency the more conflict)
*/
func YCSB(c int, s float64, w float64 ,l int, d int, is_ro bool) t_txn.Result {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.ERROR
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("r", 100000, v, l, w)
	
	const t_count = 100
	opss := make([](*(t_txn.OPS)), t_count)
	
	
	// for test hang
	// degree := make(chan int, 3)
	

	/* generate txn and reorder(or not) */
	if is_ro {
		reorder := ro.NewReorder(1, 2)
		for i := 0; i < t_count; i++ {

			ops := ycsb.NewOPS() // actually read write sequence
			reorder.Insert(*ops)
		}
		reorder.Init()
		opss = *(reorder.Sort())

	} else {
		for i := 0; i < t_count; i++ {

			ops := ycsb.NewOPS() // actually read write sequence
			opss[i] = ops
		}
	}

	start := time.Now()
	// Run at this

	wc, rc := ToroShell{}.Run(opss, c, d)
	elapsed := time.Since(start)
	return t_txn.Result{float64(t_count)/(elapsed.Seconds()), elapsed, wc, rc}
}



type ToroShell struct {}



func (t ToroShell) GetName() string {
	return "Toro"
}


func (t ToroShell) Run(opss [](*(t_txn.OPS)), c int, d int) (int, int) {

	return t.RunCore(opss, c, d, 1, 1)

}
/*
all txn algorithm must has this function for distrubted test using
*/
func (t ToroShell) RunCore(opss [](*(t_txn.OPS)), c int, d int, pn, pd float64) (int, int) {
	degree := make(chan int, d)
	toro := NewToro(2, true) // second parameter for debug! 
	core := make(chan int, c)
	var phase sync.WaitGroup 
	t_count := len(opss)
	phase.Add(t_count)
	txns := make([](*TXN), t_count)
	/* first phase */
	for i:=0; i < t_count; i++ {
		core <- 0
		go func(txn_id int) {
			
			ops := opss[txn_id]
			// PossibleWriteMap first parameter control the possble write count (actually * normal count)
			txns[txn_id] = toro.NewTXN(txn_id, ops.PossibleWriteMap(pn, pd, "p")) // install the possible read write set
			// t_log.Log(t_log.DEBUG, "possible write map:%v\n", txns[txn_id].GetReserveWriteString())
			txns[txn_id].InstallKeys()
			phase.Done()
			<- core
		}(i)
	}
	phase.Wait()
	// t_log.Log(t_log.DEBUG, "key:version list: \n %v \n", toro.KeysVersionString())

	/* second phase */
	phase.Add(t_count)
	for i := 0; i < t_count; i++ {
		degree <- 0
		go func(txn_id int) {
			for true {
				ops := opss[txn_id]
				ops.Reset()
				txn := txns[txn_id]
				is_next := true
				for true {
					var op t_txn.OP
					var ok bool
					if is_next {
						op, ok = ops.Next()
					}
					if ok == false {
						ops.Reset()
						break
					}
					key := op.Key

					if op.Is_write {
						core <- 0
						if txn.Write(key) == false {
							t_log.Log(t_log.ERROR, "unknow error in second phase txn %v write %v", txn.txn_id, key)
						} else {
							is_next = true
						}
						<- core
					} else {
						core <- 0
						if txn.Read(key) == false {
							runtime.Gosched()

							// Revert all
							// txn.Revert()
							is_next = false
							// ops.Reset()
						} else {
							is_next = true
						}
						<- core
					}
				}
				for true {
					ok := txn.CheckOtherTXNStaged()
					if ok {
						// t_log.Log(t_log.DEBUG, "txn %v has read a aborted version", txn.txn_id)
						break
					} else {
						continue
					}
				}

				if txn.Staged() == false {
					t_log.Log(t_log.ERROR, "txn %v got a impossible error", txn.txn_id)
					continue
				} else {
					break
				}
			}
			<- degree
			// t_log.Log(t_log.DEBUG, "txn %v finished\n", txn_id)
			phase.Done()
		}(i)
	}
	phase.Wait()
	return 0, toro.Read_conflict

}