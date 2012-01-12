package xsync

import (
	"testing"
	"time"
)

func TestWQueue(t *testing.T) {
	wq := NewWQueue(nil)

	go func() {
		wq.NQ(1)
		wq.NQ(2)
		time.Sleep(1e9)
		wq.NQ(3)
	}()

	done := make(chan bool)

	go func() {
		wq.DQ()
		wq.DQ()
		wq.DQ()
		done <- true
	}()

	<-done
}
