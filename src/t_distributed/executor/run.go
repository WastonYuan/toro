package executor


import (
	"t_txn"
	"t_log"
	"t_distributed/IO"
	"t_distributed/utils"
	"time"
	"t_distributed/server"
)

type Executor struct {
	recvfc chan t_txn.OPS
	// batch *(utils.OpsBatch) Executor do not need batch (once receive once send)
	sendfs *([](chan utils.ServerRequest))
	sendfc chan int
	recvfs *([](chan server.Resp))
	batch *(utils.Batch) // current dealing batch
	ops_batch *(utils.OpsBatch) // save the current dealing batch
	cache4svr *([](*server.Resp))
	// cache4cli *(list.List) cli not need cache the size control by the channel size
	show_fre int
	send_fre int

}


// recvfc sendfs sendfc recvfs show_fre
func NewExecutor(recvfc chan t_txn.OPS, sendfs *([](chan utils.ServerRequest)), sendfc chan int,
				 recvfs *([](chan server.Resp)), show_fre int, send_fre int) *Executor {
	
	l := len(*recvfs)
	cache4svr := make([](*server.Resp), l)
	for i := 0 ; i < len(cache4svr); i++ {
		cache4svr[i] = nil
	}
	ops_batch := utils.NewOpsBatch()
	return &Executor{recvfc, sendfs, sendfc, recvfs, nil, ops_batch, &cache4svr, show_fre, send_fre}
}



func (e *Executor) SendCli() {
	message := (*e.cache4svr)[0].Txn_c
	e.batch = nil
	e.sendfc <- message
}

func (e *Executor) SendSvr(command int) {
	for i := 0; i < len(*e.cache4svr); i++ {
		sq := utils.NewServerRequest(command, e.batch)
		t_log.Log(t_log.DEBUG, "COO: send svr command %v batch size: %v\n", command, e.batch.Len())
		(*e.sendfs)[i] <- *sq

	}
}


func (e *Executor) Send0Svr(command int) {
	e.batch = e.ops_batch.Move2Batch()
	sq := utils.NewServerRequest(command, e.batch)
	t_log.Log(t_log.DEBUG, "COO: send svr0 command %v batch size: %v\n", command, e.batch.Len())
	(*e.sendfs)[0] <- *sq
}


func (e *Executor) Check0Cache(command int) bool {
	i := 0
	if (*e.cache4svr)[i] != nil {// nil means there is no recv any data
		if (*e.cache4svr)[i].Command == command {// && (*e.cache4svr)[i].Txn_c > 0 {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}


/*
check the cache []Resp is all contain response with certain command
*/
func (e *Executor) CheckCache(command int) bool {
	for i := 0; i < len((*e.cache4svr)); i++ {
		if (*e.cache4svr)[i] != nil { // nil means there is no recv any data
			if (*e.cache4svr)[i].Command == command { // && (*e.cache4svr)[i].Txn_c > 0 {
				continue
			} else {
				return false
			}
		} else {
			return false
		}
	}
	return true
}


func (e *Executor) ResetCache() {
	for i := 0; i < len((*e.cache4svr)); i++ {
		(*e.cache4svr)[i] = nil
	}
}



func (e *Executor) Run() {
	d_send := time.Duration(1000000000 / e.send_fre) * time.Nanosecond // send frequency
	ticker_send := time.NewTicker(d_send)
	d_show := time.Duration(1000000000 / e.show_fre) * time.Nanosecond // show frequency
	ticker_show := time.NewTicker(d_show)

	for true {
		for i := 0; i < len(*e.recvfs); i++ {
			select {
			case m := <- (*e.recvfs)[i]: // receive from server
				(*e.cache4svr)[i] = &m
				IO.NetworkDelay(IO.IntSize)
			case nops := <- e.recvfc:  // receive from cli then push to batch
				// t_log.Log(t_log.DEBUG, "COO recv from cli\n")
				IO.NetworkDelay(nops.Capacity())
				e.ops_batch.Push(&nops)
			
			case <- ticker_send.C:
				// t_log.Log(t_log.DEBUG, "COO send exec to svr\n")
				if e.batch == nil {
					e.Send0Svr(utils.EXEC)
				}
			case <- ticker_show.C:
				// t_log.Log(t_log.DEBUG, "COO: recv svr cache: %v\n", *((*e.cache4svr)[0]))

			default:
				// check if send commit to 
				if e.Check0Cache(utils.EXEC) {
					e.SendSvr(utils.LOG)
					e.ResetCache() // the cache is srv receive cache
				}
				// check if send commit to 
				if e.CheckCache(utils.LOG) {
					e.SendSvr(utils.COMMIT)
					e.ResetCache()
				}
				// check is response to client
				if e.CheckCache(utils.COMMIT) {
					e.SendCli()
					e.ResetCache()
					e.batch = nil // batch is cli receive cache
				}
			}
		}
	}
}