package ponies

import (
	"bytes"
	"testing"
)

func TestWire(t *testing.T) {
	wire := new(bytes.Buffer)

	lineIn := LineIn(wire)
	lineOut := LineOut(wire)

	var p1 Pony
	p1.Project = "hello!"
	p1.Payload = make(map[string][]byte)
	p1.Payload["key"] = []byte("val")
	p1.Payload["key2"] = []byte("val2")

	go func() {
		lineOut <- p1
	}()

	p2 := <-lineIn

	if p1.Project != p2.Project {
		t.Error("projects don't match")
	}
}
