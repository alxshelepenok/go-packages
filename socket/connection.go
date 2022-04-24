package socket

import (
	"bufio"
	"io"
	"net"
)

type Connection struct {
	Incoming   chan []byte
	Done       chan struct{}
	connection net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
}

func Connect(proto, addr string) (*Connection, error) {
	c, err := net.Dial(proto, addr)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)

	conn := &Connection{
		Done:       make(chan struct{}),
		Incoming:   make(chan []byte),
		connection: c,
		reader:     r,
		writer:     w,
	}

	go func() {
		defer close(conn.Done)
		defer func(c net.Conn) {
			err := c.Close()
			if err != nil {
				return
			}
		}(c)
		for {
			bytes, err := conn.reader.ReadBytes('\n')
			if err == io.EOF {
				break
			}

			conn.Incoming <- bytes
		}
	}()

	return conn, nil
}

func (c *Connection) Emit(bytes []byte) {
	_, err := c.writer.Write(bytes)
	if err != nil {
		return
	}

	_, err = c.writer.Write([]byte("\n"))
	if err != nil {
		return
	}

	err = c.writer.Flush()
	if err != nil {
		return
	}
}

func (c *Connection) Close() {
	err := c.connection.Close()
	if err != nil {
		return
	}
}
