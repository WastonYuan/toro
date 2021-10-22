package calvin

import (
	"t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"t_log"
	"time"
	"t_txn"
	"t_util/qlock"
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
	const t_count = 100
	opss := make([](*(t_txn.OPS)), t_count)
	for i := 0; i < t_count; i++ {
		opss[i] = ycsb.NewOPS()
	}

	start := time.Now()
	wc, rc := CalvinShell{}.Run(opss, c, d)
	elapsed := time.Since(start)
	return t_txn.Result{float64(t_count)/(elapsed.Seconds()), elapsed, wc, rc}
}


/*
parameter:
core, skew, write_rate, len

*/


type CalvinShell struct {}

func (t CalvinShell) GetName() string {
	return "Calvin"
}

func (t CalvinShell) Run(opss [](*(t_txn.OPS)), c int, d int) (int, int) {

	calvin := NewCalvin(2, 1, true)
	core := make(chan int, c)
	t_count := len(opss)
	txns := make([](*TXN), t_count)
	

	var second_phase sync.WaitGroup 

	second_phase.Add(t_count)
	degree := make(chan int ,d)
	// for test hang
	// degree := make(chan int, 3)
	
	for i:=0; i < t_count; i++ {
		txn_id := i
		ops := opss[i]
		opss[txn_id] = ops
		// t_log.Log(t_log.DEBUG, "txn %v ops %v\n", txn_id, ops.GetString())
		// first phase reserve
		txns[txn_id] = calvin.NewTXN(txn_id, ops)
		// fmt.Println(txn)
		// t_log.Log(t_log.DEBUG, "txn %v first phase finished\n", txn_id)
	}


	for i:=0; i < t_count; i++ {
		degree <- 0 // not a queue can not gurantee sequence 
		go func(txn_id int) {
			txn := txns[txn_id]
			ops := opss[txn_id]
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
					<- degree
					t_log.Log(t_log.DEBUG, "txn %v finished\n", txn.txn_id)
					second_phase.Done()
					break
				}
			}
		}(i)
	}
			
	second_phase.Wait()

	return qlock.GetQLockWriteReadConflictAndClear()

}