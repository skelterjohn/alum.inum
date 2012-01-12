package main

import (
	"github.com/skelterjohn/alum.inum/messages"
	"log"
	"sync"
)

/*
jobs always get added to purgatory first
when they've got a corresponding worker, they can be put on the queue
*/
type Purgatory struct {
	lock           sync.Mutex
	workerlessJobs map[string]map[string][]Job
}

func NewPurgatory() (p Purgatory) {
	p.workerlessJobs = make(map[string]map[string][]Job)
	return
}

func (p Purgatory) AddJob(j Job) {
	p.lock.Lock()
	defer p.lock.Unlock()

	pr := GetProject(j.Project)

	if w, ok := pr.GetWorker(j.Name); ok && j.Version <= w.Version {
		// we've already got the worker
		jobQueue.Push <- j
		jobClaims.setJobStatus(j.JobHeader, messages.JobQueued)
		log.Println("queueing", j)
		return
	}

	log.Printf("adding %v to purgatory", j)
	jobClaims.setJobStatus(j.JobHeader, messages.JobAwaitingWorker)

	// otherwise store it and wait for the job to arrive
	prjs, ok := p.workerlessJobs[j.Project]
	if !ok {
		prjs = make(map[string][]Job)
		p.workerlessJobs[j.Project] = prjs
	}
	prjs[j.Name] = append(prjs[j.Name], j)

	var wr = messages.WorkerRequest{}
	wr.Project = j.Project
	wr.Name = j.Name
	var line messages.Line
	if line, ok = getLine(j.Conn); !ok {
		log.Printf("unknown connection w/ %v", j)
	}
	line.Out <- wr.Message()
}

func (p Purgatory) Notify(w Worker) {
	p.lock.Lock()
	defer p.lock.Unlock()

	prjs, ok := p.workerlessJobs[w.Project]
	if !ok {
		return
	}
	var newjobs []Job
	for _, j := range prjs[w.Name] {
		if j.Version <= w.Version {
			jobQueue.Push <- j
			jobClaims.setJobStatus(j.JobHeader, messages.JobQueued)
			log.Println("queueing", j)
		} else {
			newjobs = append(newjobs, j)
		}
	}
	prjs[w.Name] = newjobs
}
