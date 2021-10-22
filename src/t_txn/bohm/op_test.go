package bohm

import (
	"testing"
	"t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"t_log"
	// "time"
	// "t_txn"
)


/*
go test t_txn/bohm -v -run TestBasic
*/

func TestBasic(t *testing.T) {

	t_log.Loglevel = t_log.DEBUG
	const txn_count = 10
	bohm := NewBOHM(2, true)
	ycsb := t_benchmark.NewYcsb("r", 100, 3, 10, 0.3)
	var wg sync.WaitGroup
	
	for i := 0; i < txn_count; i++ {
		wg.Add(1)
		go func(txn_id int) {

			ops := ycsb.NewOPS()
			t_log.Log(t_log.DEBUG, "txn %v ops %v\n", txn_id, ops.GetString())
			txn := bohm.NewTXN(txn_id, ops.ReadWriteMap(true))

			txn.InstallKeys()
			wg.Done()
		}(i)
	}
	wg.Wait()
	t_log.Log(t_log.DEBUG, "%v\n", bohm.KeysVersionString())
	
}