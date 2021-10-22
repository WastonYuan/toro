package t_log

import (
	"fmt"
)

type LogConfig struct {
	Level         int
	Logfilepath   string
}

var Loglevel = 4
var Logconf LogConfig

const (
	PANIC = 0
	ERROR = 1
	WARN  = 2
	INFO  = 3
	DEBUG = 4
)

func Log(level int, msg string, val ...interface{}) {
	if Loglevel >= level {
		fmt.Printf(msg, val...)
		if level == PANIC {
			fmt.Println("\nFatal error ...")
			panic("Exiting")
		}
	}
}
