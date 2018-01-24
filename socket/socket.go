package socket

import (
	"net"
	"bufio"
)

type Socket struct {
	listener net.Listener
	reader *bufio.Reader
	writer *bufio.Writer
}

func New(proto, addr string) (*Socket, error) {
	l, err := net.Listen(proto, addr)
	if err != nil {
		return nil, err
	}

	conn, err := l.Accept()
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	sock := &Socket{
		listener: l,
		reader: r,
		writer: w,
	}

	return sock, nil
}

func (s *Socket) Listen() (chan []byte, chan struct{}) {
	done := make(chan struct{})
	incomming := make(chan []byte)

	go func() {
		defer close(done)
		for {
			bytes, err := s.reader.ReadBytes('\n')
			if err == nil {
				incomming <- bytes
			}
		}
	}()

	return incomming, done
}

func (s *Socket) Emit(bytes []byte) {
	s.writer.Write(bytes)
	s.writer.Write([]byte("\n"))
	s.writer.Flush()
}

func (s *Socket) Close() {
	s.listener.Close()
}