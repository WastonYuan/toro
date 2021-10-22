package rd

/*
go test t_txn/calvin/rd -v
*/


import (
	"t_log"
	"testing"
	"sync"
)

func TestConcurencyHA(t *testing.T) {
	// t_log.Loglevel = t_log.DEBUG
	t_log.Loglevel = t_log.PANIC
	var wg sync.WaitGroup
	r := NewRecord()
	for i:=0; i < 1000; i++ {
		wg.Add(1)
		go func(txn_id int) {
			defer wg.Done()
			is_w := false
			if txn_id % 5 == 1 {
				is_w = true
			}
			r.Reserve(txn_id, is_w)
			t_log.Log(t_log.DEBUG, r.LockListString())
		}(i)
	}
	wg.Wait()
	for j:=0; j < 1000; j++ {
		for i:=0; i < 1000; i++ {
			wg.Add(1)
			go func(txn_id int) {
				defer wg.Done()
				// before := r.LockListString()
				if r.Read(txn_id) == true {
					// t_log.Log(t_log.DEBUG, "before %v txn %v read ok\n", before, txn_id)
				}
				if r.Write(txn_id) == true {
					// t_log.Log(t_log.DEBUG, "before %v txn %v write ok\n", before, txn_id)
				}
			}(i)
		}
	}
	wg.Wait()

}

func TestCorrect(t *testing.T) {
	l := sync.Mutex{}
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.PANIC
	var wg sync.WaitGroup
	r := NewRecord()
	for i:=0; i < 10; i++ {
		wg.Add(1)
		go func(txn_id int) {
			defer wg.Done()
			is_w := false
			if txn_id % 5 == 1 {
				is_w = true
			}
			r.Reserve(txn_id, is_w)
			t_log.Log(t_log.DEBUG, r.LockListString())
		}(i)
	}
	wg.Wait()
	for j:=0; j < 100; j++ {
		for i:=0; i < 10; i++ {
			wg.Add(1)
			go func(txn_id int) {
				defer wg.Done()
				l.Lock()
				before := r.LockListString()
				if r.Read(txn_id) == true {
					t_log.Log(t_log.DEBUG, "before %v txn %v read ok\n", before, txn_id)
				}
				if r.Write(txn_id) == true {
					t_log.Log(t_log.DEBUG, "before %v txn %v write ok\n", before, txn_id)
				}
				l.Unlock()
			}(i)
		}
	}
	wg.Wait()

}