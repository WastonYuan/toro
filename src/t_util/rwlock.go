package t_util

import "fmt"
import "sync"
import "t_log"

/* 
An implementation of lock for coro
while add or release lock should give the txn_id
In the same txn id read-write locks are not mutually exclusive
In the same txn id read-write locks are independent which means while add a
write lock the read lock can be add too in same txn. and if no other txn's read
this txn there can be add the write lock too.

multi add lock just give a warning but no error
*/


var (
	RWLock_WrtieConflict = 0
	RWLock_ReadConflict = 0
)

func GetWriteReadConflictAndClear() (int, int) {
	w := RWLock_WrtieConflict
	r := RWLock_ReadConflict
	RWLock_WrtieConflict = 0
	RWLock_ReadConflict = 0
	return w, r
}

type RWlock struct {
	Rset *map[int]bool // store the lock map
	W int // write lock's txn_id, -1 is no lock
	lock *sync.Mutex // lock's lock
}

func NewRWLock() *RWlock {
	nlock := new(RWlock)
	nlock.Rset = new(map[int]bool)
	*(nlock.Rset) = map[int]bool{}
	nlock.W = -1
	nlock.lock = new(sync.Mutex)
	return nlock
}

func (rwl *RWlock) SyncRLock(txn_id int) {
	is_conflict := false
	for true {
		res := func() bool {
			rwl.lock.Lock()
			defer rwl.lock.Unlock()
			if rwl.W == txn_id || rwl.W == -1 { // no other txn write
				if (*(rwl.Rset))[txn_id] == true {
					// fmt.Printf("WARNING txn %v is already rlock\n", txn_id)
					t_log.Log(t_log.DEBUG, "WARNING txn %v is already rlock\n", txn_id)
				} else {
					(*(rwl.Rset))[txn_id] = true
				}
				return true // add lock success
			} else {
				return false // add lock failed
			}
		}()

		if res == true {
			break
		} else { // for statistic
			if is_conflict == false {
				is_conflict = true
				RWLock_ReadConflict = RWLock_ReadConflict + 1
				// fmt.Printf("DEBUG txn %v add rlock but hang by w:%v r:%v\n", txn_id, rwl.W, *rwl.Rset)
			}
		}
	}
}

func (rwl *RWlock) RUnlock(txn_id int) {
	rwl.lock.Lock()
	defer rwl.lock.Unlock()
	delete((*(rwl.Rset)), txn_id)
}

func (rwl *RWlock) Info() string{
	rwl.lock.Lock()
	defer rwl.lock.Unlock()
	s := fmt.Sprintf("w:%v, r:%v", rwl.W, *(rwl.Rset))
	return s 

}

func (rwl *RWlock) SyncLock(txn_id int) {
	is_conflict := false
	for true {
		res := func() bool {
			rwl.lock.Lock()
			defer rwl.lock.Unlock()
			// fmt.Printf("%v use SyncLock\n", txn_id)
			if ( len(*(rwl.Rset)) == 0 || ( len(*(rwl.Rset)) == 1 && (*(rwl.Rset))[txn_id] == true ) ) && (rwl.W == txn_id || rwl.W == -1)  {
				// no other txn read and no other txn write
				if rwl.W == txn_id {
					// fmt.Printf("WARNING txn %v is already lock\n", txn_id)
					t_log.Log(t_log.DEBUG, "WARNING txn %v is already lock\n", txn_id)
				} else {
					rwl.W = txn_id
				}
				return true
			} else {
				return false
			}
		}()
		if res == true {
			break
		} else { // for statistic
			if is_conflict == false {
				is_conflict = true
				RWLock_WrtieConflict = RWLock_WrtieConflict + 1
				// fmt.Printf("DEBUG txn %v add lock but hang by w:%v r:%v\n", txn_id, rwl.W, *rwl.Rset)
			}
		}
	}
}

func (rwl *RWlock) Unlock(txn_id int) {
	
	rwl.lock.Lock()
	// fmt.Printf("%v use UnLock\n", txn_id)
	defer rwl.lock.Unlock()
	if txn_id == rwl.W {
		rwl.W = -1
	} else {
		t_log.Log(t_log.ERROR, "ERROR txn %v is not lock\n", txn_id)
	}
}