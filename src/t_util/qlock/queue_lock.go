package qlock

/* mainly used by calvin */
import (
	"container/list"
	"sync"
	"fmt"
)

var (
	QLock_WrtieValidateConflict = 0
	QLock_ReadValidateConflict = 0
)

func GetQLockWriteReadConflictAndClear() (int, int) {
	w := QLock_WrtieValidateConflict
	r := QLock_ReadValidateConflict
	QLock_WrtieValidateConflict = 0
	QLock_ReadValidateConflict = 0
	return w, r
}

type lock struct {
	Txn_id int
	Is_write bool
}

type QueueLock struct {
	queue *(list.List)
	rwlock *sync.RWMutex
}

func NewQueueLock() *QueueLock {
	l := list.New()
	return &QueueLock{l, &(sync.RWMutex{})}
}


func (q *QueueLock) AddLock(txn_id int, is_write bool) {
	l := lock{txn_id, is_write}
	q.rwlock.Lock()
	defer q.rwlock.Unlock()
	q.queue.PushBack(l)
}

func (q *QueueLock) LockListString() string {
	q.rwlock.RLock()
	defer q.rwlock.RUnlock()
	var res string
	for e := q.queue.Front(); e != nil; e = e.Next() {
		// do something with e.Value
		res = res + fmt.Sprintf("%v ", e.Value)
	}
	return res
}

/*
unsync method
*/
func (q *QueueLock) ReadLock(txn_id int) bool {
	// read is also need the write lock since need to update the queue
	q.rwlock.Lock()
	defer q.rwlock.Unlock()
	for e := q.queue.Front(); e != nil; e = e.Next() {
		cur := e.Value.(lock)
		if cur.Is_write == false && cur.Txn_id == txn_id { // get the lock
			// delete the lock
			q.queue.Remove(e)
			return true
		} else if cur.Is_write == true { // until write there is no read so no get lock
			return false
		}
	}
	return false
}


func (q *QueueLock) WriteLock(txn_id int) bool {
	// read is also need the write lock since need to update the queue
	q.rwlock.Lock()
	defer q.rwlock.Unlock()
	e := q.queue.Front()
	if e == nil {
		return false
	}
	cur := e.Value.(lock)
	if cur.Is_write == true && cur.Txn_id == txn_id { // get the lock
		q.queue.Remove(e)
		return true
	} else {
		return false
	}

}

/*
check txn_id can get the lock or not
this method each key just validate onece can get the result
the txn_id should know contain write with this key(lock)
if contained write, the first in queue must be this txn's operation
if not contained write it just loop as read lock
*/
func (q *QueueLock) Validate(txn_id int, contain_write bool) bool {
	// since there is no need to update queue so just rlock enough
	q.rwlock.RLock()
	defer q.rwlock.RUnlock()
	if contain_write {
		e := q.queue.Front()
		if e == nil {
			QLock_WrtieValidateConflict = QLock_WrtieValidateConflict + 1
			return false
		}
		cur := e.Value.(lock)
		if cur.Txn_id == txn_id { // no matter read or write
			return true
		} else {
			QLock_WrtieValidateConflict = QLock_WrtieValidateConflict + 1
			return false
		}
	} else {
		for e := q.queue.Front(); e != nil; e = e.Next() {
			cur := e.Value.(lock)
			if cur.Txn_id == txn_id  { // touch (r: or w r will also return true)
				return true
			} else if cur.Is_write == false {
				continue
			} else {
				QLock_ReadValidateConflict = QLock_ReadValidateConflict + 1
				return false
			}
		}
		QLock_ReadValidateConflict = QLock_ReadValidateConflict + 1
		return false
	}
}



