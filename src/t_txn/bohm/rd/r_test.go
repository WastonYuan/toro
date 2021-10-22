package rd

import (
	"testing"
	// "t_benchmark"
	// "t_util"
	"sync"
	// "fmt"
	"t_log"
	// "time"
	// "t_txn"
)


/*
go test t_txn/bohm/rd -v -run TestBasic
*/

func TestBasic(t *testing.T) {
	t_log.Loglevel = t_log.DEBUG
	// Install
	const txn_count = 5
	const write_count = 3
	const read_count = 6
	r := NewRecord()
	var wg sync.WaitGroup
	// first phase
	for i := 0; i < txn_count; i++ {
		wg.Add(1)
		go func(txn_id int) {
			defer wg.Done()
			for j := 0; j < write_count; j++ {
				r.Install(txn_id, PENDING)
			}
			t_log.Log(t_log.DEBUG, "txn %v install ok\n", txn_id)
		}(i)
	}
	wg.Wait()
	// second phase

	for i := 0; i < txn_count; i++ {
		wg.Add(1)
		go func(txn_id int) {
			defer wg.Done()
			vs := [write_count](*Version){}
			for j:=0; j < write_count; j++ {
				vs[j] = r.Write(txn_id)
				if vs[j] == nil {
					t_log.Log(t_log.DEBUG, "txn %v write error, version list:%v\n", txn_id, r.VersionListString())
				} else {
					t_log.Log(t_log.DEBUG, "txn %v write ok, now version is %v\n", txn_id, vs[j].GetString())
				}
			}
			for j:=0 ;j < read_count; j++{
				rv := r.Read(txn_id)
				if rv == nil { // can read only my txn 1 or other txn 2
					t_log.Log(t_log.DEBUG, "txn %v read error, version list:%v\n", txn_id, r.VersionListString())
				} else {
					t_log.Log(t_log.DEBUG, "txn %v read ok, now version is %v\n", txn_id, rv.GetString())
				}
			}
			
			for j:=0; j < write_count; j++ {
				v := vs[j]
				v.UpdateStats(STAGED)
			}
			t_log.Log(t_log.DEBUG, "txn %v staged ok, version list:%v\n", txn_id, r.VersionListString())
		}(i)
	}
	wg.Wait()

}