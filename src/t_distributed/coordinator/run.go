package coordinator

import (
	"t_txn"
	"t_log"
	"t_distributed/IO"
	"t_distributed/utils"
	"container/list"
	"time"
	"t_distributed/server"
)



type Coodinator struct {
	recvfc chan t_txn.OPS
	batch *(utils.OpsBatch)
	sendfs *([](chan utils.ServerRequest))
	sendfc chan int
	recvfs *([](chan server.Resp))
	batch_size int
	cache *([](*list.List)) // for save the request from server
	show_fre int
	send_fre int
}

/*
cli2coo, coo2server, coo2cli, server2coo, batch_size, show(debug)_frequency
*/
func NewCoodinator(recvfc chan t_txn.OPS, sendfs *([](chan utils.ServerRequest)), sendfc chan int, recvfs *([](chan server.Resp)), bs int, sf int, sendf int) *Coodinator {
	ops_b := utils.NewOpsBatch()
	cache := make([](*list.List), len(*sendfs))
	for i := 0; i < len(cache); i++ {
		cache[i] = list.New()
	}
	return &(Coodinator{recvfc, ops_b, sendfs, sendfc, recvfs, bs, &cache, sf, sendf})
}

/*
recv from client
*/
func (coo *Coodinator) Receive() *(t_txn.OPS) {
	// var nops t_txn.OPS
	nops := <- coo.recvfc
	// simulated the network delay
	IO.NetworkDelay(nops.Capacity())
	return &nops
}

/*
send to client
*/
func (coo *Coodinator) SendCli(message int) {
	// var nops t_txn.OPS 
	coo.sendfc <- message
} 


func (coo *Coodinator) RunPre() {

	// d_show := time.Duration(1000000 / coo.show_fre) * time.Microsecond // show frequency
	// ticker_show := time.NewTicker(d_show)

	// count := 0 // for statistic
	// channel_dur := time.Duration(0)
	// log_dur := time.Duration(0)
	// b_insert_dur := time.Duration(0)
	for true {
		// select {
		// case <- ticker_show.C:
		// 	t_log.Log(t_log.DEBUG, "COO_Pre: Recv rate:%v, Log rate:%v, Insert rate:%v\n", cr, lr, br)
		// default:

			nops := coo.Receive()
			coo.Log(nops) // Run Log in first phase of Async 2PC
			
			coo.batch.Push(nops)
			
			// t_log.Log(t_log.DEBUG, "Coodinator receive ops: %v size %v\n", nops.GetString(), nops.Capacity())
		// }
	}
}


func (coo *Coodinator) Log(ops *t_txn.OPS) {
	// t_log.Log(t_log.DEBUG, "ops cap :%v", ops.Capacity())
	IO.LogDelay(ops.Capacity())
}



/*
send to server and recv from server all in this function
*/
func (coo *Coodinator) RunCommit() {
	d_show := time.Duration(1000000000 / coo.show_fre) * time.Nanosecond // show frequency
	ticker_show := time.NewTicker(d_show)
	d_send := time.Duration(1000000000 / coo.send_fre) * time.Nanosecond // send frequency
	ticker_send := time.NewTicker(d_send)
	batch_size := 0
	send_count := 0
	for true {
		for i := 0; i < len(*coo.recvfs); i++ {
			select { // async recv the server response
			case m := <- (*coo.recvfs)[i]:
				(*(coo.cache))[i].PushBack(m)
				IO.NetworkDelay(IO.IntSize)  
			case <- ticker_show.C:
				t_log.Log(t_log.INFO, "Coo_Commit Average Batch size: %v\n", float64(batch_size)/float64(send_count))
			case <- ticker_send.C:
			// default:
				batch_size = batch_size + coo.batch.Len()
				send_count ++
				batch := coo.batch.Move2Batch()
				for i := 0; i < len(*coo.sendfs); i++ {
					req := utils.NewServerRequest(utils.EXEC_COMMIT, batch.DeepCopy())
					(*coo.sendfs)[i] <- (*req)
				}
				// }
			}
		}
		// check if send back to client
		send := true
		for i := 0; i < len((*coo.cache)); i++ {
			if (*coo.cache)[i].Len() > 0 { // this may conflict with recv which influence the speed
				continue
			} else {
				send = false
				break
			}
		}
		var message server.Resp
		// send
		if send {
			for i := 0; i < len((*coo.cache)); i++ {
				e := (*coo.cache)[i].Front()
				message = e.Value.(server.Resp)
				(*coo.cache)[i].Remove(e)
			}
			coo.SendCli(message.Txn_c)
		}
	}
}