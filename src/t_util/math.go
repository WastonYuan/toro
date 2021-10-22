package t_util

import "math/rand"
import "time"

type Period struct {
	average float64
	variance float64
}

func NewPeriod(a float64, v float64) *Period {
	return &Period{a, v}
}

func (p Period) Wait() {
	s := NormalInt(p.average, p.variance) 
	time.Sleep(time.Duration(s) * time.Nanosecond)
}

func NormalInt(average, variance float64) int {
	return int(rand.NormFloat64() * variance +  average)
}

/*[0, upper)*/
func RandInt(upper int) int {
	return rand.Intn(upper)
}

/*from 0 to 1.0*/
func RandFloat() float64 {
	return rand.Float64()
}