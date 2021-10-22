package pwv

import (
	"t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"t_log"
	"time"
	"t_txn"
)

/*
go test t_txn/calvin -v -run TestYCSB
*/

/*
d this the second phase parallel count
which control the conflict and concurency degree (the more concurency the more conflict)
*/
func YCSB(c int, s float64, w float64 ,l int, d int) t_txn.Result {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.ERROR
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("r", 100000, v, l, w)

	const t_count = 100
	opss := make([](*(t_txn.OPS)), t_count)
	for i := 0; i < t_count; i++ {
		opss[i] = ycsb.NewOPS()
	}
	start := time.Now()
	
	wc, rc := PWVShell{}.Run(opss, c, d)

	elapsed := time.Since(start)
	return t_txn.Result{float64(t_count)/(elapsed.Seconds()), elapsed, wc, rc}
}


/*
parameter:
core, skew, write_rate, len

*/
type PWVShell struct {}

func (t PWVShell) GetName() string {
	return "PWV"
}

func (t PWVShell) Run(opss [](*(t_txn.OPS)), c int, d int) (int, int) {

	pwv := NewPWV(2, false) // second parameter for debug!
	core := make(chan int, c)
	t_count := len(opss)
	txns := make([](*TXN), t_count)
	
	degree := make(chan int, d)
	var phase sync.WaitGroup 

	phase.Add(t_count)
	// for test hang
	// degree := make(chan int, 3)
	


	/* first phase */
	for i:=0; i < t_count; i++ {
		core <- 0
		go func(txn_id int) {
			ops := opss[txn_id]
			opss[txn_id] = ops

			txns[txn_id] = pwv.NewTXN(txn_id, ops.ReadWriteMap(true))
			txns[txn_id].InstallKeys()
			phase.Done()
			<- core
		}(i)
	}
	phase.Wait()

	/* second phase */
	phase.Add(t_count)
	for i := 0; i < t_count; i++ {
		degree <- 0
		go func(txn_id int) {
			ops := opss[txn_id]
			ops.Reset()
			txn := txns[txn_id]
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
						t_log.Log(t_log.ERROR, "unknow error in second phase txn %v write %v", txn.txn_id, key)
					}
					<- core
				} else {
					core <- 0
					if txn.Read(key) == false {
						// Revert all
						txn.Revert()
						ops.Reset()
					}
					<- core
				}
			}
			for !txn.CheckOtherTXNStaged() {}
			if txn.Staged() == false {
				t_log.Log(t_log.ERROR, "unknow error in second phase staged txn %v", txn.txn_id)
			}
			<- degree 
			// t_log.Log(t_log.DEBUG, "txn %v finished\n", txn_id)
			phase.Done()
		}(i)
	}
	phase.Wait()

	return 0, pwv.Read_conflict

}