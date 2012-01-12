package main

import (
	"github.com/skelterjohn/alum.inum/xsync"
	"github.com/skelterjohn/alum.inum/messages"
	"log"
	"net"
)

var stalledJobs = xsync.NewWQueue(nil)

func flushDelegates() {
	jobs := stalledJobs.PopAll()
	for _, j := range jobs {
		log.Printf("requeueing %v", j)
		jobQueue.Push <- j
		jobClaims.setJobStatus(j.(Job).JobHeader, messages.JobQueued)
	}
	select {
	case j := <-stalledJobs.Pop:
		log.Printf("requeueing %v", j)
		jobQueue.Push <- j
		jobClaims.setJobStatus(j.(Job).JobHeader, messages.JobQueued)
	default:
	}
}

func delegateJob(j Job) {
	log.Println("too busy for", j)

	var conn *net.TCPConn
	var best Load
	loadLock.Lock()

	// since the order of map iteration is pseudo-random, this kind-of-randomly chooses
	// the destination from the set of "best" candidates
	for c, l := range loads {
		if c == j.Conn {
			// rather stick it back on the queue than send it back to where it came from
			continue
		}
		if l.FreeWorkers > best.FreeWorkers {
			best = l
			conn = c
		}
	}
	loadLock.Unlock()

	if conn != nil {
		line, exists := getLine(conn)
		if exists {
			log.Printf("outsourcing %v")
			line.Out <- j.Message()
			return
		}
	}

	log.Printf("stalling %v", j)
	stalledJobs.Push <- j
	jobClaims.setJobStatus(j.JobHeader, messages.JobStalled)
}

var busyDone chan chan bool

func delegationLoop() {
	log.Println("running delegationLoop")
	defer log.Println("terminating delegationLoop")

	var delegatedJobs = make(chan Job)

	running := true
	go func() {
		for running {
			workCond.Lock()
			workCond.Wait(0)
			workCond.Unlock()

			// all workers busy, grab the next job
			log.Printf("all workers busy")
			
			j := (<-jobQueue.Pop).(Job)

			// did we steal it from an available worker?
			workCond.Lock()
			if workCond.Value != 0 {
				// requeue it
				jobQueue.Push <- j
				workCond.Unlock()
				continue
			}
			workCond.Unlock()

			go func() {
				log.Printf("delegating %v", j)
				delegatedJobs <- j
			}()
			
		}
		workCond.Unlock()
	}()

	var reply chan bool

loop:
	for {
		select {
		case j := <-delegatedJobs:
			delegateJob(j)
		case reply = <-busyDone:
			running = false
			workCond.Broadcast(workCond.Value)
			break loop
		}
	}

	reply <- true
}
