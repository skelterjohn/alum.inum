package main

import (
	"github.com/skelterjohn/alum.inum/messages"
	"log"
)

func broadcastLoadSingle(line messages.Line) {
	log.Printf("sending load to %v", line.Conn.RemoteAddr())
	var l Load
	active, total := workerStatus()
	l.FreeWorkers = total - active
	line.Out <- l.Message()
}

func broadcastLoad() {
	lineLock.Lock()
	defer lineLock.Unlock()

	for _, line := range lines {
		broadcastLoadSingle(line)
	}
}
