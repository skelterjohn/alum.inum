package xsync

import (
	"sync"
)

type WQueue struct {
	items []interface{}
	*sync.Mutex
	*sync.Cond
	push chan interface{}
	Push chan<- interface{}
	pop  chan interface{}
	Pop  <-chan interface{}
}

func NewWQueue(lock *sync.Mutex) (wq *WQueue) {
	wq = &WQueue{}
	if lock != nil {
		wq.Mutex = lock
	} else {
		wq.Mutex = &sync.Mutex{}
	}
	wq.Cond = sync.NewCond(wq)
	wq.push = make(chan interface{})
	wq.Push = wq.push
	wq.pop = make(chan interface{})
	wq.Pop = wq.pop
	go wq.monitorPush()
	go wq.monitorPop()
	return
}

func (wq *WQueue) PopAll() (items []interface{}) {
	wq.Mutex.Lock()
	defer wq.Mutex.Unlock()
	items = wq.items
	wq.items = []interface{}{}
	return
}

func (wq *WQueue) monitorPush() {
	for i := range wq.push {
		wq.NQ(i)
	}
}

func (wq *WQueue) monitorPop() {
	for {
		wq.pop <- wq.DQ()
	}
}

func (wq *WQueue) NQ(i interface{}) {
	wq.Mutex.Lock()
	defer wq.Mutex.Unlock()
	wq.items = append(wq.items, i)
	wq.Cond.Broadcast()
}

func (wq *WQueue) DQ() (i interface{}) {
	wq.Mutex.Lock()
	defer wq.Mutex.Unlock()

	for len(wq.items) == 0 {
		wq.Cond.Wait()
	}

	i = wq.items[0]
	wq.items = wq.items[1:]

	return i
}
