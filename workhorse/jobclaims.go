package main

import (
	"errors"
	"fmt"
	"github.com/skelterjohn/alum.inum/messages"
	"log"
	"sync"
)

var jobClaims = NewJobClaims()

type JobClaims struct {
	ownedJobs map[string]int
	lock      sync.Mutex
}

func NewJobClaims() (jc *JobClaims) {
	jc = &JobClaims{
		ownedJobs: make(map[string]int),
	}
	return
}

func (jc *JobClaims) jobKey(jh messages.JobHeader) string {
	return fmt.Sprintf("%s/%s.%d/%s", jh.Project, jh.Name, jh.Version, jh.ID)
}
func (jc *JobClaims) claimJob(jh messages.JobHeader) {
	log.Printf("claiming %s", jc.jobKey(jh))
	jc.lock.Lock()
	defer jc.lock.Unlock()
	jc.ownedJobs[jc.jobKey(jh)] = messages.JobClaimed
}
func (jc *JobClaims) disownJob(jh messages.JobHeader) {
	log.Printf("disowning %s", jc.jobKey(jh))
	jc.lock.Lock()
	defer jc.lock.Unlock()
	delete(jc.ownedJobs, jc.jobKey(jh))
}
func (jc *JobClaims) isMyJob(jh messages.JobHeader) (owned bool) {
	jc.lock.Lock()
	defer jc.lock.Unlock()
	_, owned = jc.ownedJobs[jc.jobKey(jh)]
	return
}
func (jc *JobClaims) getJobStatus(jh messages.JobHeader) (s int, claimed bool) {
	jc.lock.Lock()
	defer jc.lock.Unlock()
	s, claimed = jc.ownedJobs[jc.jobKey(jh)]
	return
}
func (jc *JobClaims) setJobStatus(jh messages.JobHeader, s int) (err error) {
	jc.lock.Lock()
	defer jc.lock.Unlock()
	k := jc.jobKey(jh)
	_, claimed := jc.ownedJobs[k]
	if !claimed {
		err = errors.New("not my job")
		return
	}
	jc.ownedJobs[k] = s
	return
}
