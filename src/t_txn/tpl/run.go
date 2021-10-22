package tpl

/*
go test t_txn/tpl . -v -run Test*
*/

import (
	"t_benchmark"
	"t_util"
	"sync"
	"t_log"
	"time"
	"t_txn"
)


func YCSB(c int, s float64, w float64 ,l int) t_txn.Result {
	t_log.Loglevel = t_log.INFO
	// t_log.Loglevel = t_log.PANIC
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("t", 100000, v, l, w)
	
	const t_count = 1000
	opss := make([](*(t_txn.OPS)), t_count)

	for i := 0; i < t_count; i++ {
		opss[i] = ycsb.NewOPS()
	}
	start := time.Now()

	wc, rc := TplShell{}.Run(opss, c, 0)

	elapsed := time.Since(start)
	return t_txn.Result{float64(t_count)/(elapsed.Seconds()), elapsed, wc, rc}
}


type TplShell struct {}

func (t TplShell) GetName() string {
	return "Tpl"
}

func (t TplShell) Run(opss [](*(t_txn.OPS)), c int, d int) (int, int) {
	tpl := NewTPL(2, 100, 0, 0)
	// index := t_index.NewMmap(2)
	// sam := make(chan int, 100) // most read write concurrency
	// period := t_util.NewPeriod(0, 0)
	core := make(chan int, c)
	
	t_count := len(opss)

	var wg sync.WaitGroup 
	
	for i:=0; i < t_count; i++ {
		wg.Add(1)
		go func(txn_id int) {
			core <- 0
			ops := opss[txn_id]
			defer wg.Done()
			// 2pl need to know read write map (map is save the addr so map as para also cause concurency problem)
			txn := tpl.NewTXN(txn_id, ops.ReadWriteMap(false), ops.ReadWriteMap(true))
			// txn.PrintLockList()
			n_ops := *ops
			// n_ops.Show(txn.txn_id)
			for true {
				op, ok := n_ops.Next()
				// fmt.Println(key, is_write, ok)
				if ok == true {
					if op.Is_write == true {
						// fmt.Printf("txn %v write %v ready\n", txn.txn_id, op.Key)
						//index *(t_index.Mmap), key string, samphore chan int, period t_util.Period)
						txn.SyncWrite(op.Key)
						// fmt.Printf("txn %v write %v ok\n", txn.txn_id, op.Key)
					} else {
						// fmt.Printf("txn %v read %v ready\n", txn.txn_id, op.Key)
						txn.SyncRead(op.Key)
						// fmt.Printf("txn %v read %v ok\n", txn.txn_id, op.Key)
					}
				} else {
					t_log.Log(t_log.DEBUG, "txn %v finished\n", txn.txn_id)
					break
				}
			}
			<- core
		}(i)
	}
	wg.Wait()

	return t_util.GetWriteReadConflictAndClear()


}