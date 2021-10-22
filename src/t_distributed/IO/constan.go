package IO

import (
	"time"
)

var (
	network float64 = 10.0 // M/s
	disk float64 = 7.0 // M/s 
	LogDelay_Count = time.Duration(0)
	NetworkDelay_Count = time.Duration(0)
	CommitDelay_Count = time.Duration(0)
	Exec_Count = time.Duration(0)
	IntSize = 4
)


func LogDelay(bytes int) {
	dura := int(1000000000 / (disk * 1024 * 1024) * float64(bytes))
	LogDelay_Count = LogDelay_Count + time.Duration(dura)
	// time.Sleep(time.Duration(dura) * time.Nanosecond)
}


func NetworkDelay(bytes int) {
	dura := int(1000000000 / (network * 1024 * 1024) * float64(bytes))
	NetworkDelay_Count = NetworkDelay_Count + time.Duration(dura)
	// time.Sleep(time.Duration(dura) * time.Nanosecond)
}

func CommitDelay(bytes int) {
	dura := int(1000000000 / (disk * 1024 * 1024) * float64(bytes))
	CommitDelay_Count = CommitDelay_Count + time.Duration(dura)
	// time.Sleep(time.Duration(dura) * time.Nanosecond)
}

func AddExecTime(d *time.Duration) {
	Exec_Count = Exec_Count + *d
}