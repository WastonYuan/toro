package main

import (
	// "t_benchmark"
	// "t_util"
	// "sync"
	// "fmt"
	"t_log"
	// "time"
	"t_txn"
	"t_distributed/client"
	"t_distributed/executor"
	"t_distributed/utils"
	"t_distributed/server"
	"t_distributed/benchmark"
	// "os"
)


/*
go test t_distributed -v -run TestSync
*/

func main() {

	t_log.Loglevel = t_log.INFO

	benchmark.Flag()

	if (*benchmark.VerbosePtr) == true {
		t_log.Loglevel = t_log.DEBUG
	}

	net_cache := 1000000

	cli2coo := make(chan t_txn.OPS, net_cache)
	coo2cli := make(chan int, net_cache)
	coo2svr1 := make(chan utils.ServerRequest, net_cache) // for init server
	coo2svr2 := make(chan utils.ServerRequest, net_cache)
	coo2svr_l := [](chan utils.ServerRequest){coo2svr1, coo2svr2} // for init coodinator
	svr2coo1 := make(chan server.Resp, net_cache)
	svr2coo2 := make(chan server.Resp, net_cache)
	svr2coo_l := [](chan server.Resp){svr2coo1, svr2coo2}



	//cli2coo, coo2cli, prefix, average, variance, txn_len, write_rate, request_frequency, show_frequency
	cli := client.NewClient(cli2coo, coo2cli, "r", 10000, 100, 100, 0.3, 10000, 2)

	// recvfc sendfs sendfc recvfs show_fre send_fre
	exe := executor.NewExecutor(cli2coo, &coo2svr_l, coo2cli, &svr2coo_l, 1, 1000)

	var tgo t_txn.Tgorithm = benchmark.TxnType((*benchmark.TxnPtr))
	t_log.Log(t_log.INFO, "Type: %v\n", tgo.GetName())

	// coo2svr, svr2coo, Tgorithm
	svr1 := server.NewServer(coo2svr1, svr2coo1, tgo)
	svr2 := server.NewServer(coo2svr2, svr2coo2, tgo)

	
	
	go exe.Run()
	go svr1.Run()
	go svr2.Run()
	
	cli.Run(true, 2)
	// time.Sleep(100 * time.Second)

	
}