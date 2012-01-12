package main

import (
	"log"
	"fmt"
	"time"
	"sync"
	"github.com/skelterjohn/alum.inum/messages"
)

/*
The WatchStatus is used to track the status of all jobs that are this
workhorse's responsibility, whether they're remote or local.

If the job is remote, and the status request broadcasts don't get a response,
this workhorse needs to re-issue the job.

The watchOver function handles this process.
*/

type WatchStatus struct {
	lock sync.Mutex
	statuses map[string]int
}

func NewWatchStatus() (ws *WatchStatus) {
	ws = &WatchStatus{
		statuses: make(map[string]int),
	}
	return
}

func (ws *WatchStatus) key(jh messages.JobHeader) string {
	return fmt.Sprintf("%s/%s.%d/%s", jh.Project, jh.Name, jh.Version, jh.ID)
}

func (ws *WatchStatus) setWatchStatus(jh messages.JobHeader, status int) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	ws.statuses[ws.key(jh)] = status	
}

func (ws *WatchStatus) clearWatchStatus(jh messages.JobHeader) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	delete(ws.statuses, ws.key(jh))
}

func (ws *WatchStatus) getWatchStatus(jh messages.JobHeader) (status int, set bool) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	status, set = ws.statuses[ws.key(jh)]
	return
}

var statusWatcher = NewWatchStatus()

/*
	this function runs in the background, ensuring the job completes

	basically, every so often it sends out a ping to make sure someone
	has claimed the job. if no one responds soon enough, the job is added
	back to the queue
*/
func watchOver(j Job) {
	jh := j.JobHeader

	var jsr messages.JobStatusRequest
	jsr.JobHeader = jh

	statusMessages := make(chan messages.JobStatus)
	statusSubscriber := func(msg messages.Message, src messages.Line) (keepgoing bool) {
		typ := string(msg.Payload["type"])
		if typ != messages.JobStatusType {
			keepgoing = true
			return
		}
		js, _ := messages.JobStatusFromMessage(msg)
		jsh := js.JobHeader
		if jsh == jh {
			statusMessages <- js
		}
		//resubscribe to get the next status
		keepgoing = false
		return
	}

	msg := jsr.Message()
	msg.Broadcast = true

	isMyJob := jobClaims.isMyJob(jh)

	broadcast := func() {
		statusWatcher.clearWatchStatus(jh)	
		broadcastMessage(msg)
	}

	if true {
		// new hotness
		for {
			log.Printf("checking on %v", j)
			if !jobClaims.isMyJob(jh) {
				broadcast()

				go subscribe(statusSubscriber)

				timeout := time.NewTimer(1e9)
				defer timeout.Stop() // so if we don't pick it up, the goroutine attached to it can die

				//log.Printf("awaiting status")

				select {
				case js := <-statusMessages:
					log.Printf("%v is remote, %d", j, js.Status)
					if js.Status == messages.JobCompleted {
						return
					}
				case <-timeout.C:
					log.Printf("lost track of %v", j)
					// no status came back in time, so reclaim and requeue
					jobClaims.claimJob(jh)
					//purgatory.AddJob(j)
					go prepareJobForQueue(j)
				}
			} else {
				// it's my job
				status, ok := jobClaims.getJobStatus(jh)
				log.Printf("%v is here, %d", j, status)
				// it's my job and completed at the same time
				if ok && status == messages.JobCompleted {
					return
				}
			}
			time.Sleep(5e9)
		}
	} else {
		// old and busted
		if !isMyJob {
			broadcast()
		}

		for {
			// do this once every 5 seconds
			time.Sleep(5e9)

			log.Printf("checking on %v", j)

			// if it was my job and now isn't, send out a status request and go to sleep
			if isMyJob && !jobClaims.isMyJob(jh) {
				isMyJob = false
				broadcast()
				continue
			}

			isMyJob = jobClaims.isMyJob(jh)

			// if it's now my job, go back to sleep
			if isMyJob {
				status, ok := jobClaims.getJobStatus(jh)
				if ok && status == messages.JobCompleted {
					return
				}
				continue
			}

			// if it's not my job, check the last status report we got
			if status, set := statusWatcher.getWatchStatus(jh); set {
				if status == messages.JobCompleted {
					// job is done, stop watching
					return
				}
				// someone, in the last sleep cycle, broadcasted a status message about this job
				broadcast()
				continue
			}

			log.Printf("lost track of %v", j)
			
			// no one sent anything back, so let's send it to purgatory where it can await a worker
			jobClaims.claimJob(jh)
			isMyJob = true
			purgatory.AddJob(j)

		}
	}
}
