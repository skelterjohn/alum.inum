package messages

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"io"
)

type Message struct {
	Project    string
	Payload    map[string][]byte
	Broadcast  bool
	Signatures map[string]bool
}

func New() (m Message) {
	m.Payload = make(map[string][]byte)
	m.Signatures = make(map[string]bool)
	return
}

func (m Message) Sign(signature string) {
	m.Signatures[signature] = true
}

func (p Message) XMessage() (xp XMessage) {
	xp.Project = &p.Project
	b := p.Broadcast
	xp.Broadcast = &b
	for k, v := range p.Payload {
		s := k // since k is reused, and we use the address, we need to need to copy the reference
		xp.Payload = append(xp.Payload, &XItem{Label: &s, Data: v})
	}
	for s, v := range p.Signatures {
		if v {
			xp.Signatures = append(xp.Signatures, s)
		}
	}
	return
}

func makeMessage(xp XMessage) (p Message) {
	p.Project = *xp.Project
	p.Broadcast = *xp.Broadcast
	p.Payload = make(map[string][]byte)
	p.Signatures = make(map[string]bool)
	for _, xitem := range xp.Payload {
		p.Payload[*xitem.Label] = xitem.Data
	}
	for _, signature := range xp.Signatures {
		p.Signatures[signature] = true
	}
	return
}

func LineOut(trail io.WriteCloser) (in chan<- Message, dc chan<- bool) {
	ch := make(chan Message)
	in = ch

	dcch := make(chan bool)
	dc = dcch

	go func(trail io.WriteCloser, out <-chan Message, dc <-chan bool) {
		for {
			select {
			case p := <-out:
				xp := p.XMessage()
				data, err := proto.Marshal(&xp)
				if err != nil {
					continue
				}
				weight := uint64(len(data))
				err = binary.Write(trail, binary.BigEndian, weight)
				if err != nil {
					break
				}
				n, err := trail.Write(data)
				if err != nil {
					break
				}
				if n != int(weight) {
					break
				}
			case <-dc:
				trail.Close()
			}
		}
	}(trail, ch, dcch)

	return
}

func LineIn(trail io.Reader) (out <-chan Message) {
	ch := make(chan Message)
	out = ch
	go func(trail io.Reader, in chan<- Message) {
		for {
			var weight uint64
			err := binary.Read(trail, binary.BigEndian, &weight)
			if err != nil {
				break
			}
			buffer := make([]byte, weight)
			n, err := trail.Read(buffer)
			if n != int(weight) || err != nil {
				break
			}
			var xp XMessage
			err = proto.Unmarshal(buffer, &xp)
			if err != nil {
				break
			}
			p := makeMessage(xp)
			in <- p
		}
		close(in)
	}(trail, ch)

	return
}
