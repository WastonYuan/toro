package server

import (
	"t_distributed/utils"
	"t_log"
	"t_distributed/IO"
	// "t_txn/toro"
	"t_txn"
	"time"
)

type Resp struct {
	Command int
	Txn_c int
}

type Server struct {
	receiver chan utils.ServerRequest
	sender chan Resp
	tg t_txn.Tgorithm
}

/*
coo2svr, svr2coo, Tgorithm
*/
func NewServer(rec chan utils.ServerRequest, send chan Resp, tg t_txn.Tgorithm) *Server {
	return &(Server{rec, send, tg})
}


func (s *Server) Receive() (int, *(utils.Batch)) {
	req := <- s.receiver
	command, batch := req.Get()
	// network delay
	IO.NetworkDelay(batch.Capacity())
	return command, batch
}

func (s *Server) Send(message Resp) {
	s.sender <- message
} 


func (s *Server) Commit(b *utils.Batch) {
	IO.CommitDelay(b.CommitSize())
}

func (s *Server) Run() {
	
	for true {
		command, batch := s.Receive()
		if command == utils.EXEC_COMMIT {
			// t_log.Log(t_log.DEBUG, "Server rec batch: %v cap: %v\n" , batch.GetString(), batch.Capacity())
			t_log.Log(t_log.DEBUG, "Server rec exec_commit batch cap: %v, len %v\n", batch.Capacity(), batch.Len())
			start := time.Now()
			s.tg.Run(*(batch.Batch2Slice()), 3, 3) // Exec
			elapsed := time.Since(start)
			IO.AddExecTime(&elapsed)
			s.Commit(batch) // Commit
		} else if command == utils.EXEC {
			// t_log.Log(t_log.DEBUG, "Server rec batch: %v cap: %v\n" , batch.GetString(), batch.Capacity())
			t_log.Log(t_log.DEBUG, "Server rec exec batch cap: %v, len %v\n", batch.Capacity(), batch.Len())

			start := time.Now()
			s.tg.Run(*(batch.Batch2Slice()), 3, 3) // Exec
			elapsed := time.Since(start)
			IO.AddExecTime(&elapsed)

		} else if command == utils.LOG {
			t_log.Log(t_log.DEBUG, "Server rec log batch cap: %v, len %v\n", batch.Capacity(), batch.Len())
			IO.LogDelay(batch.Capacity())
		} else if command == utils.COMMIT {
			t_log.Log(t_log.DEBUG, "Server rec commit batch cap: %v, len %v\n", batch.CommitSize(), batch.Len())
			s.Commit(batch) // Commit
		} 
		s.Send(Resp{command, batch.Len()})
	}
}