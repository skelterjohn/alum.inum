package main

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/skelterjohn/alum.inum/messages"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

/*
unzipping requires an io.ReaderAt, so here one is
*/
type BytesReaderAt []byte

func (br BytesReaderAt) ReadAt(buf []byte, offset int64) (n int, err error) {
	o := int(offset)
	n = len(buf)
	if o+n > len(br) {
		n = len(br) - o
	}
	copy(buf, br[o:o+n])
	return
}

/*
unzips the worker's source
*/
func makeWorker(w Worker) (err error) {
	if _, ok := workerFuncs[w.Language]; !ok {
		err = errors.New(fmt.Sprintf("unsupported language: %s", w.Language))
		return
	}

	dir := w.SrcDir()

	if _, werr := os.Stat(dir); werr == nil {
		//it exists
		return
	}

	log.Printf("unzipping %v", w)

	os.RemoveAll(dir)

	var reader *zip.Reader
	reader, err = zip.NewReader(BytesReaderAt(w.Data), int64(len(w.Data)))
	if err != nil {
		return
	}

	for _, f := range reader.File {
		var zf io.ReadCloser
		zf, err = f.Open()
		if err != nil {
			panic(err)
		}

		fpath := filepath.Join(dir, f.Name)

		err = os.MkdirAll(filepath.Dir(fpath), 0755)
		if err != nil {
			panic(err)
		}

		var rf io.Writer
		rf, err = os.Create(fpath)
		if err != nil {
			return
		}

		_, err = io.Copy(rf, zf)
		if err != nil {
			return
		}

		zf.Close()
	}

	return
}

var workerFuncs = map[string]func(string, string) error{
	"python": runPythonWorker,
}

/*
run a job locally
*/
func runJob(w Worker, j Job) (err error) {
	log.Printf("starting %v", j)

	err = makeWorker(w)
	if err != nil {
		return
	}

	wf := workerFuncs[w.Language]

	dir := filepath.Join(projectDirectory, w.Project, "workers", w.Name, j.ID)

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}

	if j.Input != nil {
		var inputFile io.WriteCloser
		inputFile, err = os.Create(filepath.Join(dir, "input"))
		if err != nil {
			return
		}

		_, err = inputFile.Write(j.Input)
		if err != nil {
			return
		}

		inputFile.Close()
	}

	relsrc := filepath.Join("..", "src", fmt.Sprintf("%d", w.Version))

		jobClaims.setJobStatus(j.JobHeader, messages.JobRunning)
	err = wf(dir, relsrc)

	if err == nil {
		jobClaims.setJobStatus(j.JobHeader, messages.JobCompleted)
		statusWatcher.setWatchStatus(j.JobHeader, messages.JobCompleted)
		log.Printf("completed %v", j)
		jc := messages.JobConf{j.JobHeader}
		msg := jc.Message()
		msg.Sign(serverID)
		msg.Broadcast = true
		broadcastMessage(msg)
	} else {
		log.Printf("error with %v: %v", j, err)
	}

	return
}

/*
run dir/worker.py
*/
func runPythonWorker(jobdir, relsrc string) (err error) {
	env := os.Environ()
	env = append(env, fmt.Sprintf("PYTHONPATH=%s", relsrc))
	cmd := exec.Command("python", filepath.Join(relsrc, "worker.py"))
	cmd.Dir = jobdir
	cmd.Env = env
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	return
}
