package rd

import (
	"container/list"
	"sync"
	// "t_log"
	// "fmt"
)

type Record struct {
	vl *(list.List) // with version from old to new
	a_vid int // for allocate vid
	rwlock *(sync.RWMutex) // the read lock no must use since only first phase write but not read
}

func NewRecord() *Record {
	l := list.New()
	return &( Record{l, 0, &(sync.RWMutex{})} )
}

/*
The Write operation in the first phase
this different from the mvcc
mvcc need validate the read txn_id is larger than this txn_id
Toro do not need to worry (validate) since write only occur in first phase which contained read operation
Write: new a version and insert to right position
Only write will change the version List (read change version)
*/
func (r *Record) Install(txn_id int, stats int) *Version {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	nv := NewVersion(r.a_vid, txn_id, txn_id, stats) // rts = wts
	r.a_vid = r.a_vid + 1
	var e *(list.Element)
	for e = r.vl.Front(); e != nil; e = e.Next() {
		// do something with e.Value
		v := e.Value.(*Version)
		if v.wts > txn_id {
			break
		}
	}
	if e == nil {
		r.vl.PushBack(nv)
	} else {
		r.vl.InsertBefore(nv, e)
	}
	return nv
}

func (r *Record) VersionListString() string {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	var res string
	for e := r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		res = res + v.GetString()
	}
	return res
} 

/*
find pending point and validate txn_id
logically it will not return nil
*/
func (r *Record) Write(txn_id int) *Version {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	for e := r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		// txn_id == v.wts the version will not be abort stats (ABORT only occur in the txn is finished and read by other txn)
		if v.stats == PENDING && txn_id == v.wts {
			v.UpdateStats(MODIFIED)
			return v
		}
	}
	// PWV should add staged updated
	return nil
}


/*
it may return false depend on the write is ok
if read suceess version != nil
if bool == true means need fetch a COMMIT read record
*/
func (r *Record) Read(txn_id int) (*Version, bool) {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	var e *(list.Element) = nil
	var pre *(list.Element)
	for e = r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		// ignore the ABORTED stats version so the chosse version including localVisible (once meet must read)
		// and other (pending) visible all will not be aborted stats
		if v.GetStats() == ABORT { 
			// t_log.Log(t_log.DEBUG, "txn %v read Abort v %v and skip\n", txn_id, v.GetString())
			continue
		}
		if v.IsLocalVisible(txn_id) {
			v.Read(txn_id)
			return v, false
		}
		if v.LessTXN(txn_id) == false { // if true the verion is other pending visible
			// read before the first can not visible 
			break
		}
		pre = e
	}
	if pre == nil { // no visible version need fetch
		// nv := r.Install(-1, COMMITED)
		return nil, true
	} else {
		v := pre.Value.(*Version)
		if v.IsOtherVisible(txn_id) {
			v.Read(txn_id)
			return v, false
		} else { // has a pending other visible version
			return nil, false
		}
	}

}