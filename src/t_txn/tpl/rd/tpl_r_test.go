package rd

/*
go test t_txn/tpl/rd -v tpl_r_test.go
*/

import (
	"testing"
	"fmt"
	"t_util"
	"sync"
	"math/rand"
	"time"
)


/* test write can be read in same txn */

func TestReadWriteInTXN(t *testing.T) {
	var rv [](*Record)
	var r_count = 2 // configure record count
	for i:=0 ; i < r_count; i++ { // the key is i
		r := NewRecord()
		rv = append(rv, r)
	}

	
	sam := make(chan int, 3) // configure core count
	p := t_util.NewPeriod(1000, 0) // configre read write period
	var wg sync.WaitGroup 
	for i:=0 ; i < 10; i++ { // configure txn count
		wg.Add(1)
		go func(txn_id int, period t_util.Period) {
			ri := rand.Intn(r_count)
			r := rv[ri]
			fmt.Printf("\nenter txn %v\n", txn_id)
			r.SyncWrite(sam, txn_id, period, false)
			fmt.Printf("\ntxn %v write %v ok\n", txn_id, ri)
			r.SyncRead(sam, txn_id, period, false)
			fmt.Printf("\ntxn %v read %v ok\n", txn_id, ri)
			r.SyncRead(sam, txn_id, period, true)
			fmt.Printf("\ntxn %v read %v ok\n", txn_id, ri)
			r.SyncWrite(sam, txn_id, period, true)
			fmt.Printf("\ntxn %v write %v ok\n", txn_id, ri)
			fmt.Printf("\nexit txn %v\n", txn_id)
			wg.Done()
		}(i, p)
	}

	go func() {
		for true {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf(".")
		}
	}() // visual the time spend
	wg.Wait()
	

}


/*
test syncWrite syncRead is ok
configure para including cucurency count and cores and period
*/

func TestRandWriteRead(t *testing.T) {

	var rv [](*Record)
	var r_count = 2 // configure record count
	for i:=0 ; i < r_count; i++ {
		r := NewRecord() // the key is i
		rv = append(rv, r)
	}

	
	sam := make(chan int, 3) // configure core count
	p := t_util.NewPeriod(1000, 0) // configre read write period
	var wg sync.WaitGroup 
	for i:=0 ; i < 10; i++ { // configure txn count
		wg.Add(1)
		go func(txn_id int, period t_util.Period) {
			ri := rand.Intn(r_count)
			r := rv[ri]
			r.SyncWrite(sam, txn_id, period, true)
			fmt.Printf("\ntxn %v write %v ok\n", txn_id, ri)
			r.SyncRead(sam, txn_id, period, true)
			fmt.Printf("\ntxn %v read %v ok\n", txn_id, ri)
			wg.Done()
		}(i, p)
	}

	go func() {
		for true {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf(".")
		}
	}() // visual the time spend
	wg.Wait()
}
