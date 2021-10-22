package client


import (
	"t_txn"
	"t_benchmark"
	"time"
	"t_log"
	"t_distributed/IO"
	"fmt"
)

type Client struct {

	sender chan t_txn.OPS // send to coodinator
	recv chan int
	ycsb *t_benchmark.Ycsb
	request_f int
	show_f int


	// statistic 
	recv_txn_count int
}

/*
parameter:
cli2coo, coo2cli, prefix, average, variance, txn_len, write_rate, request_frequency, show_frequency
*/
func NewClient(send chan t_txn.OPS, recv chan int, p string, a float64, v float64, l int, w float64, rf int, sf int) *Client {
	ycsb := t_benchmark.NewYcsb(p, a, v, l, w)
	return &(Client{send, recv, ycsb, rf, sf, 0})
}


/*
receive should wait acorrding to the size of ops
*/
func (c *Client) Send() {
	ops := c.ycsb.NewOPS()
	nops := *ops
	c.sender <- nops
}


func (c *Client) Run(is_sync bool, server_count int) {
	
	t_unit_sec := 60 // the computer time unit (1/~ sec)
	d_send := time.Duration(1000000 / t_unit_sec) * time.Microsecond // request frequency split to every 1/60 sec
	ticker_send := time.NewTicker(d_send)
	d_show := time.Duration(1000000 / c.show_f) * time.Microsecond
	ticker_show := time.NewTicker(d_show)
	start := time.Now()
	send_spend := time.Duration(0)
	count := 0
	for true {
		select {
		case <- ticker_send.C:
			
			s_start := time.Now()
			for i := 0; i < c.request_f / t_unit_sec; i++ {
				count ++
				c.Send()
			}
			s_elapsed := time.Since(s_start)
			send_spend = send_spend + s_elapsed
		case m := <- c.recv:
			t_log.Log(t_log.DEBUG, "Client recv commit: %v\n", m)
			IO.NetworkDelay(IO.IntSize)
			c.recv_txn_count = c.recv_txn_count + m
		case t := <- ticker_show.C:

			hour, min, sec := t.Clock()
			t_str := fmt.Sprintf("%v:%v:%v", hour, min, sec)

			if is_sync == true {
				elapsed := time.Since(start)
				dura := elapsed + IO.LogDelay_Count + IO.NetworkDelay_Count + IO.CommitDelay_Count
				log_rate := ( float64(IO.LogDelay_Count.Seconds()) / float64(server_count) ) / float64(dura.Seconds())
				net_rate := float64(IO.NetworkDelay_Count.Seconds()) / float64(dura.Seconds())
				commit_rate := ( float64(IO.CommitDelay_Count.Seconds()) / float64(server_count) ) / float64(dura.Seconds())
				exec_rate := float64(IO.Exec_Count.Seconds()) / float64(dura.Seconds())
				
				
				tps := float64(c.recv_txn_count)/(dura.Seconds())
				t_log.Log(t_log.INFO, "Client: %v tps: %v Log rate: %v Network rate: %v Commit rate: %v Exec rate: %v Other: %v\n", t_str, tps, log_rate, net_rate,
					commit_rate, exec_rate, 1 - log_rate - net_rate - commit_rate - exec_rate)

			} else {
				elapsed := time.Since(start)
				net_dur_s := IO.NetworkDelay_Count.Seconds() // all net delay should pay without parallel
				log_dura_s := IO.LogDelay_Count.Seconds() / float64(server_count)
				exec_commit_dura_s := ( IO.Exec_Count.Seconds() + IO.CommitDelay_Count.Seconds() ) / float64(server_count)
				log_exec_commit_dura_s := 0.0
				if log_dura_s > exec_commit_dura_s {
					log_dura_s = log_dura_s - exec_commit_dura_s
					log_exec_commit_dura_s = exec_commit_dura_s
					exec_commit_dura_s = 0.0
				} else {
					exec_commit_dura_s = exec_commit_dura_s - log_dura_s
					log_exec_commit_dura_s = log_dura_s
					log_dura_s = 0
				}
				dura_s := elapsed.Seconds() + net_dur_s + log_dura_s + exec_commit_dura_s + log_exec_commit_dura_s
				tps := float64(c.recv_txn_count)/(dura_s)
				log_rate := log_dura_s / dura_s
				net_rate := net_dur_s / dura_s
				commit_exec_rate := exec_commit_dura_s / dura_s
				log_or_exe_commit_rate := log_exec_commit_dura_s / dura_s
				
				t_log.Log(t_log.INFO, "Client time: %v tps: %v Log rate: %v Network rate: %v CommitAndExec rate: %v LogOrCommitExec rate: %v Other: %v\n", t_str, tps, log_rate, net_rate,
					commit_exec_rate, log_or_exe_commit_rate, 1 - log_rate - net_rate - commit_exec_rate - log_or_exe_commit_rate)
			}
		}
	}
}



