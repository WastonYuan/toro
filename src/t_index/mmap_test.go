package t_index

/*
go test t_index -v mmap_test.go
*/

import (
	"testing"
	"t_txn/tpl/rd"
	"fmt"
	// "t_util"
	"sync"
	"strconv"
	// "math/rand"
	// "time"
	"t_util"
)

/* the record should be set to 2pl */
/* test write can be read in same txn */

func TestBasic(t *testing.T) {

	sam := make(chan int, 5)
	p := t_util.NewPeriod(1000, 0) // configre read write period
	index := NewMmap( 5 )
	txn_count := 30
	var wg sync.WaitGroup 
	for i :=0 ; i < txn_count ; i++ {
		wg.Add(1)
		go func(txn_id int) {
			// write(insert)
			r := rd.NewRecord()
			wkey := "foo_" + strconv.Itoa(txn_id)
			index.Insert(wkey, r)
			fmt.Printf("txn %v write %v ok\n", txn_id, wkey)

			//read 
			rkey := "foo_" + strconv.Itoa(t_util.RandInt(txn_count))
			var ok bool
			r, ok = index.Search(rkey).(*rd.Record)
			if !ok {
				fmt.Printf("txn %v read key %v but not found\n", txn_id, rkey)
			} else {
				r.SyncRead(sam, txn_id, *p, true)
				fmt.Printf("txn %v read key %v ok\n", txn_id, rkey)
			}

			//write check and insert/write
			wkey = "foo_" + strconv.Itoa(t_util.RandInt(txn_count))
			r, ok = index.Search(wkey).(*rd.Record)
			if !ok {
				r = rd.NewRecord()
				r = index.GetOrInsert(wkey, r).(*rd.Record)
			}
			r.SyncWrite(sam, txn_id, *p, true)
			fmt.Printf("txn %v write key %v ok\n", txn_id, wkey)

			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Printf("read conflict :%v write conflict: %v\n", t_util.RWLock_ReadConflict, t_util.RWLock_WrtieConflict)
	// index.Show()

}