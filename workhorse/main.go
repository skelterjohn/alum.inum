package main

import (
	"bufio"
	"fmt"
	"github.com/skelterjohn/alum.inum/messages"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	Handshake = "ponyexpress"
	SoloPort  = 1324
)

var lineLock sync.Mutex
var lines = make(map[*net.TCPConn]messages.Line)

func getLine(conn *net.TCPConn) (mline messages.Line, ok bool) {
	lineLock.Lock()
	mline, ok = lines[conn]
	lineLock.Unlock()
	return
}

var idLock sync.Mutex
var connIDs = make(map[*net.TCPConn]string)
var IDConns = make(map[string]*net.TCPConn)

var loadLock sync.Mutex
var loads = make(map[*net.TCPConn]Load)

var projectDirectory string = "projects"

var purgatory = NewPurgatory()

var serverID string

var ustrs <-chan string
func doUstrs() {
	unum := 0
	ch := make(chan string)
	ustrs = ch
	go func() {
		for {
			unum++
			ustr := fmt.Sprintf("%s.%d", serverID, unum)
			ch <- ustr
		}
	}()
}

func serve(port string) {
	laddr, err := net.ResolveTCPAddr("tcp", "localhost:"+port)
	if err != nil {
		panic(err)
	}
	mlines := messages.Serve(laddr)
	log.Printf("listening on %v", laddr)

	for mline := range mlines {
		lineLock.Lock()
		lines[mline.Conn] = mline
		lineLock.Unlock()

		mline.Out <- Ident{serverID}.Message()
		broadcastLoadSingle(mline)

		go func() {
			handleMessages(mline)
			lineLock.Lock()
			delete(lines, mline.Conn)
			lineLock.Unlock()
		}()
	}
}

func connect(raddrString string) {
	raddr, err := net.ResolveTCPAddr("tcp", raddrString)
	if err != nil {
		panic(err)
	}
	var mline messages.Line
	for {
		mline, err = messages.Connect(raddr)
		if err == nil {
			break
		}
		log.Println(err)
		time.Sleep(2e9)
	}

	lineLock.Lock()
	lines[mline.Conn] = mline
	lineLock.Unlock()

	mline.Out <- Ident{serverID}.Message()

	broadcastLoadSingle(mline)

	handleMessages(mline)

	lineLock.Lock()
	delete(lines, mline.Conn)
	lineLock.Unlock()
}

func quit() {
	log.Println("quitting")
	lineLock.Lock()
	for _, mline := range lines {
		mline.Disconnect()
	}
	lineLock.Unlock()
}

func main() {
	// only use one processor for the workhorse - the rest are for workers
	runtime.GOMAXPROCS(1)

	if len(os.Args) == 2 {
		serverID = os.Args[1]
	} else {
		serverID = "x"
	}

	go doUstrs()

	log.Printf("workhorse %s ready", serverID)

	brin := bufio.NewReader(os.Stdin)

run:
	for {
		lineb, pref, err := brin.ReadLine()
		if err == io.EOF {
			lineb = []byte("quit")
			fmt.Println(err)
			pref = false
		}
		if pref {
			fmt.Println("line too long")
		}
		line := string(lineb)
		tokens := strings.Fields(line)
		if len(tokens) == 0 { continue }
		switch tokens[0] {
		case "connect":
			if len(tokens) != 2 {
				fmt.Println("usage: connect <host>:<port>")
				continue
			}
			go connect(tokens[1])
		case "serve":
			if len(tokens) != 2 {
				fmt.Println("usage: serve <port>")
				continue
			}
			go serve(tokens[1])
		case "start":
			if len(tokens) != 2 {
				fmt.Println("usage: start <id>")
				continue
			}
			go workLoop(tokens[1])
		case "stop":
			if len(tokens) != 2 {
				fmt.Println("usage: stop <id>")
				continue
			}
			workCond.Lock()
			done, ok := workLoopTerminators[tokens[1]]
			delete(workLoopTerminators, tokens[1])
			if ok {
				reply := make(chan bool)
				done <- reply
				<-reply
			} else {
				fmt.Printf("no such worker: %s\n", tokens[1])
			}
			workCond.Unlock()
		case "delegate":
			if busyDone != nil {
				fmt.Println("already delegating")
			} else {
				busyDone = make(chan chan bool)
				go delegationLoop()
			}
		case "status":
			active, total := workerStatus()
			log.Printf("%d / %d\n", active, total)
		case "quit":
			quit()
			break run
		default:
			fmt.Println("don't understand:", line)
		}
	}

	for _, ch := range workLoopTerminators {
		reply := make(chan bool)
		ch <- reply
		<-reply
	}

	if busyDone != nil {
		r := make(chan bool)
		busyDone <- r
		<-r
	}
}
