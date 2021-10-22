package t_util

import "sync"

type SeqHandler struct {
	size int
	acc_count int // now access txn count
	max_txn_id int
	rwlock *(sync.RWMutex)

}

func NewSeqHandler(s, m int) *SeqHandler {
	return &SeqHandler{s, 0, m, &(sync.RWMutex{})}
}


func (sh *SeqHandler) Access(txn_id int) bool {
	if sh.acc_count < sh.size {
		if sh.max_txn_id + 1 == txn_id {
			// get access
			sh.max_txn_id = txn_id
			sh.acc_count = sh.acc_count + 1
			return true
		}
	}
	return false
}

