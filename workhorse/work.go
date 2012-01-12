package main

import (
	"github.com/skelterjohn/alum.inum/messages"
	"github.com/skelterjohn/alum.inum/xsync"
	"log"
)

// all jobs go through this queue
var jobQueue = xsync.NewWQueue(nil)

// here are jobs for which we are awaiting a worker
var backLog = xsync.NewWQueue(nil)

// signals down these channels terminate the corresponding workLoops
var workLoopTerminators = make(map[string]chan<- chan bool)
// tracks workers are active
var workStatus = make(map[string]bool)

var workCond = xsync.NewIntCond(nil)

/*
see how many workers there are, and how many are busy
*/
func workerStatus() (active, total int) {
	workCond.Lock()
	total = len(workStatus)
	active = total - workCond.Value
	workCond.Unlock()
	return
}

/*
grabs jobs from the queue
- if we've got the job's worker, execute it
- if we haven't, send out the worker request, put job on backlog
*/
func workLoop(id string) {
	log.Printf("running workLoop %s", id)
	done := make(chan chan bool)
	workCond.Lock()
	workLoopTerminators[id] = done
	workStatus[id] = false
	workCond.Value++
	workCond.Broadcast(workCond.Value)
	workCond.Unlock()

	broadcastLoad()
	flushDelegates()

	var reply chan bool

loop:
	for {
		select {
		case ji := <-jobQueue.Pop:

			workCond.Lock()
			workCond.Value--
			workCond.Broadcast(workCond.Value)
			workStatus[id] = true
			workCond.Unlock()

			broadcastLoad()
			flushDelegates()

			j := ji.(Job)
			p := GetProject(j.Project)
			w, ok := p.GetWorker(j.Name)
			if !ok || j.Version > w.Version {
				backLog.Push <- j
				continue
			}
			runJob(w, j)

			workCond.Lock()
			workCond.Value++
			workCond.Broadcast(workCond.Value)
			workStatus[id] = false
			workCond.Unlock()

			broadcastLoad()
			flushDelegates()

		case reply = <-done:
			break loop
		}
	}

	log.Printf("terminating workLoop %s", id)
	workCond.Lock()
	workCond.Value--
	workCond.Broadcast(workCond.Value)
	delete(workLoopTerminators, id)
	delete(workStatus, id)
	workCond.Unlock()

	broadcastLoad()
	flushDelegates()

	reply <- true
}

/*
pull jobs from the backlog and request their workers
*/
func workerRequestLoop() {
	for {
		j := (<-backLog.Pop).(Job)
		wr := messages.WorkerRequest{
			Project: j.Project,
			Name:    j.Name,
		}
		mline, ok := getLine(j.Conn)
		if !ok {
			log.Println("job with no connection")
		}

		mline.Out <- wr.Message()
	}
}
