package main

import (
	"github.com/skelterjohn/alum.inum/xsync"
	"log"
	"os"
	"sync"
)

var projects = make(map[string]Project)

type Project struct {
	lock sync.Mutex

	Workers map[string]Worker
	// the jobs waiting for the worker with the given key name
	Jobs *xsync.WQueue
}

func GetProject(project string) (p Project) {
	p, ok := projects[project]
	if !ok {
		p = Project{
			Workers: make(map[string]Worker),
		}
		p.Jobs = xsync.NewWQueue(&p.lock)
		projects[project] = p
	}
	return
}

func (p Project) AddWorker(w Worker) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if oldw, ok := p.Workers[w.Name]; !ok || oldw.Version < w.Version {
		dir := w.SrcDir()
		if _, err := os.Stat(dir); err == nil {
			log.Printf("removing old src for %v", w)
			os.RemoveAll(dir)
		}
		p.Workers[w.Name] = w
	}
}

func (p Project) GetWorker(name string) (w Worker, ok bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	w, ok = p.Workers[name]
	return
}
