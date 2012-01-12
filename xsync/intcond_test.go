package xsync

import (
	"testing"
)

func TestIntCond(t *testing.T) {
	ic := NewIntCond(nil)

	go func() {
		ic.Lock()
		ic.Value = 4
		ic.Signal()
		ic.Unlock()

		ic.Lock()
		ic.Value = 3
		ic.Signal()
		ic.Unlock()
	}()

	done := make(chan bool)

	go func() {
		ic.Lock()
		ic.WaitValue(3)
		ic.Unlock()
		done <- true
	}()
	<-done
}
