package main

import (
	"github.com/skelterjohn/alum.inum/messages"
	"log"
	"net"
	"sync"
)


type SubscribeCallback func(msg messages.Message, src messages.Line) (keepgoing bool)
type MsgSrc struct {
	msg messages.Message
	src messages.Line
}

var subscriberChans = make(map[*int]chan MsgSrc)
var subscriberLock sync.Mutex
func subscribe(foo SubscribeCallback) {
	k := new(int) // just a unique pointer for a key
	ch := make(chan MsgSrc)
	subscriberLock.Lock()
	subscriberChans[k] = ch
	subscriberLock.Unlock()

	for msgsrc := range ch {
		if !foo(msgsrc.msg, msgsrc.src) {
			subscriberLock.Lock()
			delete(subscriberChans, k)
			subscriberLock.Unlock()
			break
		}
	}
}

func handleMessages(mline messages.Line) {
	log.Println("connected to", mline.Conn.RemoteAddr())
	defer log.Println("lost connection to", mline.Conn.RemoteAddr())

	for msg := range mline.In {
		go func(msg messages.Message, conn *net.TCPConn) {
			if msg.Broadcast {
				broadcastMessage(msg)
			}

			// toss this to subscribers
			go func(msg messages.Message, line messages.Line) {
				subscriberLock.Lock()
				for _, ch := range subscriberChans {
					ch <- MsgSrc{msg, line}
				}
				subscriberLock.Unlock()
			}(msg, mline)

			typ, ok := msg.Payload["type"]

			if !ok {
				log.Println("received message with no type")
				return
			}
			switch string(typ) {
			case "load":
				l, err := LoadFromMessage(msg, conn)
				if err != nil {
					log.Println(err)
					return
				}
				recvLoad(l)
			case "ident":
				i, err := IdentFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvIdent(i, conn)
			case messages.WorkerType:
				w, err := messages.WorkerFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvWorker(Worker{w, conn})
			case messages.WorkerRequestType:
				w, err := messages.WorkerRequestFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvWorkerRequest(WorkerRequest{w, conn})
			case messages.JobType:
				j, err := messages.JobFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvJob(Job{j, conn})
			case messages.JobAckType:
				ja, err := messages.JobAckFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvJobAck(ja)
			case messages.JobStatusRequestType:
				jsr, err := messages.JobStatusRequestFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvJobStatusRequest(jsr)
			case messages.JobStatusType:
				js, err := messages.JobStatusFromMessage(msg)
				if err != nil {
					log.Println(err)
					return
				}
				recvJobStatus(js)
			}
		}(msg, mline.Conn)
	}
}

func broadcastMessage(m messages.Message) {
	m.Sign(serverID)
	idLock.Lock()
	defer idLock.Unlock()
	lineLock.Lock()
	defer lineLock.Unlock()
	log.Printf("broadcasting %s", m.Payload["type"])
	for conn, line := range lines {
		id, ok := connIDs[conn]
		if !ok || !m.Signatures[id] {
			line.Out <- m
		}
	}
}

func recvLoad(l Load) {
	log.Printf("received %v", l)

	loadLock.Lock()
	loads[l.Conn] = l
	loadLock.Unlock()

	flushDelegates()
}

func recvIdent(i Ident, conn *net.TCPConn) {
	idLock.Lock()
	connIDs[conn] = i.ID
	IDConns[i.ID] = conn
	idLock.Unlock()
}

func recvWorker(w Worker) {
	p := GetProject(w.Project)
	log.Printf("received %v", w)
	p.AddWorker(w)
	purgatory.Notify(w)
}

func recvWorkerRequest(wr WorkerRequest) {
	log.Printf("received %v", wr)

	p := GetProject(wr.Project)
	w, ok := p.GetWorker(wr.Name)
	if !ok || w.Version < wr.Version {
		log.Println("received request for unknown worker/version")
		return
	}

	mline, ok := getLine(wr.Conn)
	if !ok {
		panic("got a worker request from an unknown connection")
	}

	msg := w.Message()
	mline.Out <- msg
}

func recvJob(j Job) {
	log.Printf("received %v", j)
	if j.ID == "" {
		j.ID = <-ustrs

		//watch over projects that we give IDs to
		log.Printf("watching %v", j)
		go watchOver(j)
	}
	//p := GetProject(j.Project)
	jobClaims.claimJob(j.JobHeader)
	ja := messages.JobAck{j.JobHeader}
	if line, ok := getLine(j.Conn); ok {
		line.Out <- ja.Message()
	}

	if false {
		// old and busted
		purgatory.AddJob(j)
	} else {
		// new hotness
		go prepareJobForQueue(j)
	}
}

func recvJobAck(ja messages.JobAck) {
	log.Printf("received ack{%s/%s.%d/%s}", ja.Project, ja.Name, ja.Version, ja.ID)
	jobClaims.disownJob(ja.JobHeader)
}

func recvJobStatusRequest(jsr messages.JobStatusRequest) {
	log.Printf("received job status request")
	if s, ok := jobClaims.getJobStatus(jsr.JobHeader); ok {
		var js messages.JobStatus
		js.JobHeader = jsr.JobHeader
		js.Status = int(s)
		msg := js.Message()
		msg.Broadcast = true
		broadcastMessage(msg)
	} else {
		log.Printf("not my job")
	}
}

func recvJobStatus(jsr messages.JobStatus) {
	log.Printf("received job status request")
	statusWatcher.setWatchStatus(jsr.JobHeader, jsr.Status)
}
