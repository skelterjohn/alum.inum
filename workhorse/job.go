package main

import (
	"time"
	"log"
	"github.com/skelterjohn/alum.inum/messages"
)

// new way to handle job life cycle

// this function ensures that the job has the right worker before queuing it
func prepareJobForQueue(j Job) {

	jobClaims.claimJob(j.JobHeader)
	ja := messages.JobAck{j.JobHeader}
	if line, ok := getLine(j.Conn); ok {
		line.Out <- ja.Message()
	}

	pr := GetProject(j.Project)

	// first, ensure that we've got the worker
	alreadyRequested := false
	waitforworker:
	for {
		w, ok := pr.GetWorker(j.Name)
		if ok && j.Version <= w.Version {
			// we've already got the right worker
			break
		}
		workerChan := make(chan Worker)
		workerSub := func(msg messages.Message, line messages.Line) (keepgoing bool) {
			if string(msg.Payload["type"]) != messages.WorkerType {
				return true
			}

			worker, err := messages.WorkerFromMessage(msg)
			if err != nil {
				return true
			}

			if worker.Project != j.Project || worker.Name != j.Name {
				return true
			}

			if worker.Version < j.Version {
				return true
			}

			// this is our worker
			w := Worker{worker, line.Conn}
			pr.AddWorker(w)
			workerChan <- w
			// stop this subscription
			return false
		}

		if !alreadyRequested {
			alreadyRequested = true
			log.Printf("requesting worker for %v", j)
			wr := messages.WorkerRequest{
				Project: j.Project,
				Name: j.Name,
				Version: j.Version,
			}
			line, ok := getLine(j.Conn)
			if !ok {
				log.Printf("job came from lost source")
			}
			line.Out <- wr.Message()
		}

		go subscribe(workerSub)
		timeout := time.NewTimer(2e9)
		select {
		case w = <- workerChan:
			break waitforworker
		case <-timeout.C:
			/*
			this timeout is here because we might have received the worker between the first
			check and when the subscription went active
			*/
		}
		timeout.Stop()
	}
	log.Printf("enqueuing %v", j)

	jobQueue.Push <- j
}