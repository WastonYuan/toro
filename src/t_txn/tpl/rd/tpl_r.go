package rd

// import "fmt"
import "t_util" 

/* 2PL record and operation */

type Record struct {
	rwlock *t_util.RWlock
}

func NewRecord() *Record {
	return &Record{t_util.NewRWLock()}
}

/* 
samphore for control the core count
txn_id for add lock
*/
func (tplr Record) SyncRead(samphore chan int, txn_id int, period t_util.Period, release_lock bool) {
	
	tplr.rwlock.SyncRLock(txn_id)
	// read period
	samphore <- 0
	period.Wait()
	<- samphore
	if release_lock {
		tplr.rwlock.RUnlock(txn_id)
	}
	
}

func (tplr Record) LockInfo() string {
	return tplr.rwlock.Info()
}

func (tplr *Record) SyncWrite(samphore chan int, txn_id int, period t_util.Period, release_lock bool) {
	
	tplr.rwlock.SyncLock(txn_id)
	// run period
	samphore <- 0
	period.Wait()
	<- samphore

	if release_lock {
		tplr.rwlock.Unlock(txn_id)
	}
	
}

func (tplr *Record) SyncLock(txn_id int) {
	tplr.rwlock.SyncLock(txn_id)
}

func (tplr *Record) SyncRLock(txn_id int) {
	tplr.rwlock.SyncRLock(txn_id)
}