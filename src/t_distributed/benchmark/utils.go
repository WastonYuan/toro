package benchmark

import (
	"t_txn"
	"t_txn/aria"
	"t_txn/toro"
	"t_txn/tpl"
	"t_txn/bohm"
	"t_txn/calvin"
	"t_txn/pwv"
	"flag"
)

func TxnType(t string) t_txn.Tgorithm {
	if t == "Toro" || t == "toro" {
		return toro.ToroShell{}
	} else if t == "Aria" || t == "aria" {
		return aria.AriaShell{}
	} else if t == "Tpl" || t == "tpl" {
		return tpl.TplShell{}
	} else if t == "Bohm" || t == "bohm" {
		return bohm.BohmShell{}
	} else if t == "Calvin" || t == "calvin" {
		return calvin.CalvinShell{}
	} else if t == "PWV" || t == "pwv" || t == "Pwv" {
		return pwv.PWVShell{}
	} else {
		return toro.ToroShell{}
	}
}



var (
	TxnPtr *string
	VerbosePtr *bool
)


func Flag() {
	TxnPtr = flag.String("txn", "toro", "string in: [\"tpl\", \"calvin\", \"bohm\", \"pwv\", \"toro\", \"aria\"]")
	
	VerbosePtr = flag.Bool("v", false, "verbose in [\"true\", \"false\"]")

	flag.Parse()
}