package t_log

/*
go test t_log -v infolog_test.go
*/

import (
	"testing"
	"time"
)

func DoSomething() (err error) {

	Log(DEBUG, "This function prints at DEBUG log level")

	return err
}

func TestBasic(t *testing.T) {

	level := DEBUG
	
	Loglevel = level
	
	go DoSomething()
	time.Sleep(10 * time.Second)


}