package main

import (
	"errors"
	"fmt"
	"github.com/skelterjohn/alum.inum/messages"
	"net"
	"path/filepath"
)

type Worker struct {
	messages.Worker
	Conn *net.TCPConn
}

func (w Worker) String() string {
	return fmt.Sprintf("w{%s/%s.%d}", w.Project, w.Name, w.Version)
}

func (w Worker) SrcDir() string {
	return filepath.Join(projectDirectory, w.Project, "workers", w.Name, "src", fmt.Sprintf("%d", w.Version))
}

type WorkerRequest struct {
	messages.WorkerRequest
	Conn *net.TCPConn
}

func (wr WorkerRequest) String() string {
	return fmt.Sprintf("wr{%s/%s.%d}", wr.Project, wr.Name, wr.Version)
}

type Job struct {
	messages.Job
	Conn *net.TCPConn
}

func (j Job) String() string {
	return fmt.Sprintf("j{%s/%s.%d/%s}", j.Project, j.Name, j.Version, j.ID)
}

/*
 information about load on a particular workhorse
*/
type Load struct {
	// number of free workers
	FreeWorkers int
	// whose load
	Conn *net.TCPConn
}

func (l Load) String() string {
	return fmt.Sprintf("l{%v,%d}", l.Conn.RemoteAddr(), l.FreeWorkers)
}

func LoadFromMessage(m messages.Message, conn *net.TCPConn) (l Load, err error) {
	if string(m.Payload["type"]) != "load" {
		err = errors.New(fmt.Sprintf("wrong type for Load: %s", m.Payload["type"]))
		return
	}

	l.FreeWorkers = messages.VarIntInt(m.Payload["workers"])

	l.Conn = conn

	return
}

func (l Load) Message() (m messages.Message) {
	m = messages.New()
	m.Payload["type"] = []byte("load")
	m.Payload["workers"] = messages.VarIntBytes(l.FreeWorkers)
	return
}

type Ident struct {
	ID string
}

func IdentFromMessage(m messages.Message) (i Ident, err error) {
	if string(m.Payload["type"]) != "ident" {
		err = errors.New(fmt.Sprintf("wrong type for Ident: %s", m.Payload["type"]))
		return
	}
	i.ID = m.Project
	return
}

func (i Ident) Message() (m messages.Message) {
	m = messages.New()
	m.Project = i.ID
	m.Payload["type"] = []byte("ident")
	return
}
