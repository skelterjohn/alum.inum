package messages

import (
	"net"
)

const (
	handshake = "ponyExpress"
)

func writeHandshake(conn *net.TCPConn) {
	conn.Write([]byte(handshake))
}

func readHandshake(conn *net.TCPConn) bool {
	// receive handshake
	buf := make([]byte, len(handshake))
	var n int
	n, err := conn.Read(buf)
	if err != nil || n != len(handshake) || string(buf) != handshake {
		return false
	}
	return true
}

type Line struct {
	Conn *net.TCPConn
	In   <-chan Message
	Out  chan<- Message
	dc   chan<- bool
}

func (line Line) Disconnect() {
	line.dc <- true
}

func Connect(raddr *net.TCPAddr) (l Line, err error) {
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return
	}

	writeHandshake(conn)
	if !readHandshake(conn) {
		panic("bad handshake")
	}

	l.Conn = conn
	l.In = LineIn(conn)
	l.Out, l.dc = LineOut(conn)

	return
}

func handleClient(conn *net.TCPConn, linesOut chan<- Line) {
	if !readHandshake(conn) {
		panic("bad handshake")
	}
	writeHandshake(conn)

	var sl Line

	sl.Conn = conn
	sl.In = LineIn(conn)
	sl.Out, sl.dc = LineOut(conn)

	linesOut <- sl
}

func Serve(laddr *net.TCPAddr) (lines <-chan Line) {
	ch := make(chan Line)
	lines = ch

	go func() {
		listener, err := net.ListenTCP("tcp", laddr)
		if err != nil {
			panic(err)
		}

		for {
			var conn *net.TCPConn

			conn, err = listener.AcceptTCP()
			if err != nil {
				panic(err)
			}

			go handleClient(conn, ch)
		}
	}()

	return
}
