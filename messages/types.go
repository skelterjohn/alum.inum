package messages

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	WorkerType           = "work"
	WorkerRequestType    = "work req"
	JobType              = "job"
	JobConfType          = "job conf"
	JobAckType           = "job ack"
	JobStatusType        = "job status"
	JobStatusRequestType = "job status request"
)


const (
	JobClaimed = iota
	JobAwaitingWorker
	JobQueued
	JobStalled
	JobRunning
	JobCompleted
)

func VarIntBytes(x int) (buf []byte) {
	buf = make([]byte, 8)
	l := binary.PutVarint(buf, int64(x))
	buf = buf[:l]
	return
}

func VarIntInt(buf []byte) (x int) {
	x64, _ := binary.Varint(buf)
	x = int(x64)
	return
}

type Worker struct {
	Project, Name string
	Language      string
	Version       int
	Data          []byte
}

func WorkerFromMessage(m Message) (w Worker, err error) {
	if string(m.Payload["type"]) != WorkerType {
		err = errors.New(fmt.Sprintf("wrong type for Worker: %s", m.Payload["type"]))
		return
	}

	w.Project = m.Project
	w.Name = string(m.Payload["name"])
	w.Language = string(m.Payload["lang"])
	w.Data = m.Payload["data"]
	w.Version = VarIntInt(m.Payload["version"])
	//w.Version, err = strconv.Atoi(string(m.Payload["version"]))

	return
}

func WorkerFromDirectory(project, name, language string, version int, dir string) (w Worker) {
	w.Project = project
	w.Name = name
	w.Language = language
	w.Version = version

	dir = filepath.Clean(dir)

	zipBuffer := bytes.NewBuffer([]byte{})
	zipWriter := zip.NewWriter(zipBuffer)

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if path == dir {
			return nil
		}
		if strings.HasPrefix(path, dir) {
			fname := path[len(dir):]
			if len(fname) > 0 && (fname[0] == '/' || fname[0] == '\\') {
				fname = fname[1:]

				fwr, zerr := zipWriter.Create(fname)
				if zerr != nil {
					panic(zerr)
				}

				realfile, zerr := os.Open(path)
				if zerr != nil {
					panic(zerr)
				}

				_, zerr = io.Copy(fwr, realfile)
				if zerr != nil {
					panic(zerr)
				}
			}
		}
		return nil
	})
	zipWriter.Close()

	w.Data = zipBuffer.Bytes()

	return
}

func (w Worker) Message() (m Message) {
	m = New()
	m.Project = w.Project
	m.Payload["type"] = []byte(WorkerType)
	m.Payload["name"] = []byte(w.Name)
	m.Payload["lang"] = []byte(w.Language)
	m.Payload["version"] = VarIntBytes(w.Version)
	m.Payload["data"] = w.Data
	return
}

/*
A request for the named worker
*/
type WorkerRequest struct {
	Project, Name string
	Version       int
}

func WorkerRequestFromMessage(m Message) (wr WorkerRequest, err error) {
	if string(m.Payload["type"]) != WorkerRequestType {
		err = errors.New(fmt.Sprintf("wrong type for WorkerRequest: %s", m.Payload["type"]))
		return
	}

	wr.Project = m.Project
	wr.Name = string(m.Payload["name"])
	wr.Version = VarIntInt(m.Payload["version"])

	return
}

func (wr WorkerRequest) Message() (m Message) {
	m = New()
	m.Project = wr.Project
	m.Payload["type"] = []byte(WorkerRequestType)
	m.Payload["name"] = []byte(wr.Name)
	m.Payload["version"] = VarIntBytes(wr.Version)
	return
}

type JobHeader struct {
	Project, Name string
	Version       int
	ID            string
}

func JobHeaderFromMessage(m Message) (j JobHeader, err error) {
	j.Project = m.Project
	j.Name = string(m.Payload["name"])
	j.Version = VarIntInt(m.Payload["version"])
	j.ID = string(m.Payload["id"])
	return
}

func (j JobHeader) Message() (m Message) {
	m = New()
	m.Project = j.Project
	m.Payload["name"] = []byte(j.Name)
	m.Payload["version"] = VarIntBytes(j.Version)
	m.Payload["id"] = []byte(j.ID)
	return
}

type JobConf struct {
	JobHeader
}

func JobConfFromMessage(m Message) (jc JobConf, err error) {
	if string(m.Payload["type"]) != JobConfType {
		err = errors.New(fmt.Sprintf("wrong type for Job: %s", m.Payload["type"]))
		return
	}

	jc.JobHeader, err = JobHeaderFromMessage(m)

	return
}

func (jc JobConf) Message() (m Message) {
	m = jc.JobHeader.Message()
	m.Payload["type"] = []byte(JobConfType)
	return
}

type JobAck struct {
	JobHeader
}

func JobAckFromMessage(m Message) (jc JobAck, err error) {
	if string(m.Payload["type"]) != JobAckType {
		err = errors.New(fmt.Sprintf("wrong type for Job: %s", m.Payload["type"]))
		return
	}

	jc.JobHeader, err = JobHeaderFromMessage(m)

	return
}

func (jc JobAck) Message() (m Message) {
	m = jc.JobHeader.Message()
	m.Payload["type"] = []byte(JobAckType)
	return
}

/*
Request for a single job
*/
type Job struct {
	JobHeader
	// this input data is passed to the job in a file called input.data
	Input []byte
}

func JobFromMessage(m Message) (j Job, err error) {
	if string(m.Payload["type"]) != JobType {
		err = errors.New(fmt.Sprintf("wrong type for Job: %s", m.Payload["type"]))
		return
	}

	j.JobHeader, err = JobHeaderFromMessage(m)

	j.Input, _ = m.Payload["input"]

	return
}

func (j Job) Message() (m Message) {
	m = j.JobHeader.Message()
	m.Payload["type"] = []byte(JobType)
	if j.Input != nil {
		m.Payload["input"] = j.Input
	}
	return
}

type JobStatus struct {
	JobHeader
	Status int
}

func JobStatusFromMessage(m Message) (j JobStatus, err error) {
	if string(m.Payload["type"]) != JobStatusType {
		err = errors.New(fmt.Sprintf("wrong type for Job: %s", m.Payload["type"]))
		return
	}

	j.JobHeader, err = JobHeaderFromMessage(m)

	j.Status = VarIntInt(m.Payload["status"])

	return
}

func (j JobStatus) Message() (m Message) {
	m = j.JobHeader.Message()
	m.Payload["type"] = []byte(JobStatusType)
	m.Payload["status"] = VarIntBytes(j.Status)
	return
}

type JobStatusRequest struct {
	JobHeader
}

func JobStatusRequestFromMessage(m Message) (j JobStatusRequest, err error) {
	if string(m.Payload["type"]) != JobStatusRequestType {
		err = errors.New(fmt.Sprintf("wrong type for Job: %s", m.Payload["type"]))
		return
	}

	j.JobHeader, err = JobHeaderFromMessage(m)

	return
}

func (j JobStatusRequest) Message() (m Message) {
	m = j.JobHeader.Message()
	m.Payload["type"] = []byte(JobStatusRequestType)
	return
}
