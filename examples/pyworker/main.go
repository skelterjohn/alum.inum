package main

import (
	"fmt"
	"github.com/skelterjohn/alum.inum/messages"
	"net"
	"time"
)

func main() {
	raddr, err := net.ResolveTCPAddr("net", "localhost:1234")
	if err != nil {
		panic(err)
	}

	var line messages.Line
	for {
		line, err = messages.Connect(raddr)
		if err != nil {
			fmt.Println(err)
			time.Sleep(2e9)
		} else {
			break
		}
	}

	var j messages.Job
	j.Project = "john"
	j.Name = "py1"
	j.Version = 2

	w := messages.WorkerFromDirectory("john", "py1", "python", 2, "py1")

	msg := w.Message()
	line.Out <- msg
	for i := 0; i < 1; i++ {
		ji := j
		//ji.ID = fmt.Sprintf("%d", i)
		line.Out <- ji.Message()
	}

	for msg := range line.In {
		switch string(msg.Payload["type"]) {
		case messages.JobAckType:
			ja, _ := messages.JobAckFromMessage(msg)
			var jsr messages.JobStatusRequest
			jsr.JobHeader = ja.JobHeader
			msg = jsr.Message()
			msg.Broadcast = true
			line.Out <- msg
		case messages.JobStatusType:
			js, _ := messages.JobStatusFromMessage(msg)
			fmt.Println(js.JobHeader, js.Status)
		}
	}
}
