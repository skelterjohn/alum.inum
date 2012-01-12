package xsync

import (
	"sync"
)

type IntCond struct {
	Value int
	*sync.Cond
	*sync.Mutex
}

func NewIntCond(lock *sync.Mutex) (ic *IntCond) {
	if lock == nil {
		lock = &sync.Mutex{}
	}
	ic = new(IntCond)
	ic.Mutex = lock
	ic.Cond = sync.NewCond(ic.Mutex)
	return
}

func (ic *IntCond) Wait(v int) {
	for ic.Value != v {
		ic.Cond.Wait()
	}
}

func (ic *IntCond) Signal(v int) {
	ic.Value = v
	ic.Cond.Signal()
}

func (ic *IntCond) Broadcast(v int) {
	ic.Value = v
	ic.Cond.Broadcast()
}
