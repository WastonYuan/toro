package tpl

/*
go test t_txn/tpl . -v -run Test*
*/

import (
	"testing"
	"t_benchmark"
	"t_util"
	"sync"
	"fmt"
	"t_log"
	"time"
)


func YCSB(c int, s float64, w float64 ,l int, t *testing.T) {
	// t_log.Loglevel = t_log.INFO
	t_log.Loglevel = t_log.PANIC
	v := 1 / s
	ycsb := t_benchmark.NewYcsb("t", 100, v, l, w)
	tpl := NewTPL(2, 100, 0, 0)
	// index := t_index.NewMmap(2)
	// sam := make(chan int, 100) // most read write concurrency
	// period := t_util.NewPeriod(0, 0)
	core := make(chan int, c)
	t_count := 10000

	var wg sync.WaitGroup 
	start := time.Now()
	for i:=0; i < t_count; i++ {
		wg.Add(1)
		go func(txn_id int) {
			core <- 0
			ops := ycsb.NewOPS()
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
					t_log.Log(t_log.INFO, "txn %v finished\n", txn.txn_id)
					break
				}
			}
			<- core
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("WriteConflict: %v, ReadConflict: %v, TakeTime: %v, OPS = %v\n", t_util.RWLock_WrtieConflict, t_util.RWLock_ReadConflict, elapsed, float64(t_count)/(elapsed.Seconds()))
}
func TestTPCC_c1_r(b *testing.T) {YCSB(1, 0.2, 0.2, 100, b) }
func TestTPCC_c2_r(b *testing.T) {YCSB(2, 0.2, 0.2, 100, b) }
func TestTPCC_c4_r(b *testing.T) {YCSB(4, 0.2, 0.2, 100, b) }
func TestTPCC_c8_r(b *testing.T) {YCSB(8, 0.2, 0.2, 100, b) }
func TestTPCC_c16_r(b *testing.T) {YCSB(12, 0.2, 0.2, 100, b) }
func TestTPCC_c32_r(b *testing.T) {YCSB(36, 0.2, 0.2, 100, b) }

// fmt.Printf("========= core loop ok =========\n")
func TestTPCC_c1_w(b *testing.T) {YCSB(1, 0.8, 0.8, 100, b) }
func TestTPCC_c2_w(b *testing.T) {YCSB(2, 0.8, 0.8, 100, b) }
func TestTPCC_c4_w(b *testing.T) {YCSB(4, 0.8, 0.8, 100, b) }
func TestTPCC_c8_w(b *testing.T) {YCSB(8, 0.8, 0.8, 100, b) }
func TestTPCC_c16_w(b *testing.T) {YCSB(12, 0.8, 0.8, 100, b) }
func TestTPCC_c32_w(b *testing.T) {YCSB(36, 0.8, 0.8, 100, b) }

// fmt.Printf("========= core loop ok =========\n")

func TestYCSB_win_s00(b *testing.T) {YCSB(32, 0.0, 0.9, 100, b) }
func TestYCSB_win_s02(b *testing.T) {YCSB(32, 0.2, 0.9, 100, b) }
func TestYCSB_win_s04(b *testing.T) {YCSB(32, 0.4, 0.9, 100, b) }
func TestYCSB_win_s06(b *testing.T) {YCSB(32, 0.6, 0.9, 100, b) }
func TestYCSB_win_s08(b *testing.T) {YCSB(32, 0.8, 0.9, 100, b) }
func TestYCSB_win_s10(b *testing.T) {YCSB(32, 1.0, 0.9, 100, b) }

// fmt.Printf("========= YCSB write intensive ok =========\n")

func TestYCSB_rin_s00(b *testing.T) {YCSB(32, 0.0, 0.1, 100, b) }
func TestYCSB_rin_s02(b *testing.T) {YCSB(32, 0.2, 0.1, 100, b) }
func TestYCSB_rin_s04(b *testing.T) {YCSB(32, 0.4, 0.1, 100, b) }
func TestYCSB_rin_s06(b *testing.T) {YCSB(32, 0.6, 0.1, 100, b) }
func TestYCSB_rin_s08(b *testing.T) {YCSB(32, 0.8, 0.1, 100, b) }
func TestYCSB_rin_s10(b *testing.T) {YCSB(32, 1.0, 0.1, 100, b) }

// fmt.Printf("========= YCSB read intensive ok =========\n")