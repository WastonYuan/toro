package utils

import (
	"container/list"
	"sync"
	"t_txn"
)

/* This batch can be modified */
type OpsBatch struct {
	opss *list.List // store *OPS
	rwlock *sync.RWMutex 
}


/* This batch is read only batch */
type Batch struct {
	opss *list.List // store *OPS
}

/*
only Async need this (Sync exec only send to one server)
*/
func (b *Batch) DeepCopy() *Batch {
	l := list.New()
	for e := b.opss.Front(); e != nil; e = e.Next() {
		ops := *(e.Value.(*t_txn.OPS))
		l.PushBack(&ops)
	}
	return NewBatch(l)
}


var (
	EXEC_COMMIT int = 0
	EXEC = 1
	LOG = 2
	COMMIT = 3
)


type ServerRequest struct {
	command int
	batch *Batch
}

func (sr *ServerRequest) Get() (int, *Batch) {
	return sr.command, sr.batch
} 

func (sr *ServerRequest) GetBatch() *Batch {
	return sr.batch
}

func NewServerRequest(command int, batch *Batch) *ServerRequest {
	return &(ServerRequest{command, batch})
}

func NewSingleOpsBatch(ops *t_txn.OPS) *Batch {
	l := list.New()
	l.PushBack(ops)
	return NewBatch(l)
}

func NewBatch(l *list.List) *Batch {
	return &Batch{l}
}

func NewServerOpsRequest(command int, ops *t_txn.OPS) *ServerRequest {
	l := list.New()
	l.PushBack(ops)
	return NewServerRequest(command, NewBatch(l))
}

func NewOpsBatch() *OpsBatch {
	l := list.New()
	return &(OpsBatch{l, &(sync.RWMutex{})})
}


func (oq *OpsBatch) Push(ops *t_txn.OPS) {
	oq.rwlock.Lock()
	defer oq.rwlock.Unlock()
	oq.opss.PushBack(ops)
}


func (oq *OpsBatch) Move2Batch() *Batch {
	oq.rwlock.Lock()
	defer oq.rwlock.Unlock()
	m := NewBatch(oq.opss)
	oq.opss = list.New()
	return m
}


func (oq *OpsBatch) Len() int {
	oq.rwlock.RLock()
	defer oq.rwlock.RUnlock()
	return oq.opss.Len()
}


func (b *Batch) GetString() string {
	var res string
	for e := b.opss.Front(); e != nil; e = e.Next() {
		ops := e.Value.(*t_txn.OPS)
		res = res + ops.GetString() + " "
	}
	return res
}


func (b *Batch) CommitSize() int {
	sum := 0
	for e := b.opss.Front(); e != nil; e = e.Next() {
		ops := e.Value.(*t_txn.OPS)
		sum = sum + ops.CommitSize()
	}
	return sum

}


func (b *Batch) Capacity() int {
	sum := 0
	for e := b.opss.Front(); e != nil; e = e.Next() {
		ops := e.Value.(*t_txn.OPS)
		sum = sum + ops.Capacity()
	}
	return sum
}


func (b *Batch) Batch2Slice() *([](*(t_txn.OPS))) {
	l := b.opss.Len()
	s := make([](*(t_txn.OPS)), l)
	var i int = 0
	for e := b.opss.Front(); e != nil; e = e.Next() {
		ops := e.Value.(*t_txn.OPS)
		s[i] = ops
		i++
	}
	return &s
} 


func (b *Batch) Len() int {
	return b.opss.Len()
}


