package calvin

/*
go test t_txn/calvin -v -run TestLockReserveValidate
*/

import (
	"testing"
	"t_benchmark"
	"t_log"
	"sync"
)

/*
New several TXN and output the lock string
*/
func TestLockReserveValidate(t *testing.T) {
	batch_size := 10
	txns := make([](*TXN), batch_size)
	txns_lock := sync.Mutex{}
	t_log.Loglevel = t_log.DEBUG
	ycsb := t_benchmark.NewYcsb("t", 100, 30, 10, 0.4)
	calvin := NewCalvin(5, 1, true) // change second para can control fp concurency, thread only right with 1 
	keys := map[string]bool{}
	var wg sync.WaitGroup
	for i := 0; i < batch_size; i++ {
		wg.Add(1)
		ops := ycsb.NewOPS()
		// t_log.Log(t_log.DEBUG, "ops key %v\n",ops.Keys())
		for true { // collect keys
			op, ok := ops.Next()
			if ok == false {
				ops.Reset()
				break
			}
			key := op.Key
			keys[key] = true
		}
		// t_log.Log(t_log.DEBUG, "txn %v ops %v", i , ops.GetString())
		go func(txn_id int) {
			txn := calvin.NewTXN(txn_id, ops)
			txns_lock.Lock()
			txns[txn_id] = txn
			txns_lock.Unlock()
			// t_log.Log(t_log.DEBUG, "txn %v keys %v\n", txn.txn_id, txn.GetKeys())
			wg.Done()
		}(i)
	}
	wg.Wait()
	// check lock
	t_log.Log(t_log.DEBUG, "calvin lock string: %v", calvin.KeysLockString())
	// t_log.Log(t_log.DEBUG, "validate keys test\n")
	for i := 0; i < batch_size; i++ {
		txn := txns[i]
		if txn.ValidateKeys() == true {
			t_log.Log(t_log.DEBUG, "txn %v validate ok\n", txn.txn_id)
		}
	}
	
	for key, _ := range keys {
		// t_log.Log(t_log.DEBUG, "key %v lock %v\n", key, calvin.GetLockString(key))
		// check validate
		for i := 0; i < batch_size; i++ {
			txn := txns[i]
			if txn.Validate(key) {
				// t_log.Log(t_log.DEBUG, "txn %v validate write %v ok\n", txn.txn_id, key)
			} else if txn.Validate(key) {
				// t_log.Log(t_log.DEBUG, "txn %v validate read %v ok\n", txn.txn_id, key)
			}
		}
		for i := 0; i < batch_size; i++ {
			txn := txns[i]
			if txn.Write(key) {
				// t_log.Log(t_log.DEBUG, "txn %v write %v ok\n", txn.txn_id, key)
			} else if txn.Read(key) {
				// t_log.Log(t_log.DEBUG, "txn %v read %v ok\n", txn.txn_id, key)
			}
		}

    }
}