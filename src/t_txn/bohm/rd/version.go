package rd

import (
	"sync"
	"fmt"
)


/*
Version Stats
*/
const (
	ABORT       = -1
	PENDING int = 0
	MODIFIED    = 1
	STAGED      = 2
	COMMITED    = 3
	
)


type Version struct {
	vid int
	wts int // wts not change once set
	// rts int // save the max read txn the rts can not be revert !!!
	// this use for garbage collection in toro
	// rts must >= wts
	stats int
	rwlock *sync.RWMutex
}

func (v Version) GetStats() int {
	return v.stats
}

func NewVersion(vid int, wts int, rts int, stats int) *Version {
	return &Version{vid, wts, stats, &(sync.RWMutex{})}
}

func (v *Version) UpdateStats(stats int) {
	v.rwlock.Lock()
	defer v.rwlock.Unlock()
	v.stats = stats
}

/*
all visiable version will return true
so the record gurantee the read order (old to new)
*/
func (v *Version) IsOtherVisible(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	// validate
	// this version write by other txn or write by this txn
	// write by other txn condition can be adapt to this txn
	if v.wts < txn_id && v.stats >= STAGED {
		// v.rts = txn_id
		return true
	} else {
		return false
	}
}

/*
check is readable by same txn
*/
func (v *Version) IsLocalVisible(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	if v.wts == txn_id && v.stats == MODIFIED {
		return true
	} else {
		return false
	}
}

func (v *Version) Read(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	if v.wts < txn_id && v.stats >= STAGED || v.wts == txn_id {
		return true
	} else {
		return false
	}
}

func (v1 *Version) LessTXN(txn_id int) bool {
	return v1.wts < txn_id
}


func (v1 *Version) GetString() string {
	return fmt.Sprintf("[%v %v %v]", v1.vid, v1.wts, v1.stats)
}


