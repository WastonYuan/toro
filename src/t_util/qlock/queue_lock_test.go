package qlock

/*
go test t_util/qlock -v -run QueueLock
*/


import (
	"t_log"
	"testing"
	"sync"
)

func TestQueueLockConcurency(t *testing.T) {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.PANIC
	var wg sync.WaitGroup
	ql := NewQueueLock()
	for i:=0; i < 10; i++ {
		wg.Add(1)
		go func(txn_id int) {
			defer wg.Done()
			is_w := false
			if txn_id % 5 == 1 {
				is_w = true
			}
			ql.AddLock(txn_id, is_w)
			t_log.Log(t_log.DEBUG, ql.LockListString())
		}(i)
	}
	wg.Wait()
	for j:=0; j < 1; j++ {
		for i:=0; i < 10; i++ {
			wg.Add(1)
			go func(txn_id int) {
				defer wg.Done()
				before := ql.LockListString()
				if ql.Validate(txn_id) {
					t_log.Log(t_log.DEBUG, "txn %v validate ok\n ", txn_id)
				}
				if ql.ReadLock(txn_id) == true {
					t_log.Log(t_log.DEBUG, "before %v txn %v read ok\n", before, txn_id)
				}
				if ql.WriteLock(txn_id) == true {
					t_log.Log(t_log.DEBUG, "before %v txn %v write ok\n", before, txn_id)
				}
			}(i)
		}
	}
	wg.Wait()
}