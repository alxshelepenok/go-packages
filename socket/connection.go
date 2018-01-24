package socket

import (
	"io"
	"net"
	"bufio"
)

type Connection struct {
	Incomming chan []byte
	Done chan struct{}
	connection net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func Connect(proto, addr string) (*Connection, error) {
	c, err := net.Dial(proto, addr)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)

	conn := &Connection{
		Done: make(chan struct{}),
		Incomming: make(chan []byte),
		connection: c,
		reader: r,
		writer: w,
	}

	go func() {
		defer close(conn.Done)
		defer c.Close()
		for {
			bytes, err := conn.reader.ReadBytes('\n')
			if err == io.EOF {
				break
			}

			conn.Incomming <- bytes
		}	
	}()

	return conn, nil
}

func (c *Connection) Emit(bytes []byte) {
	c.writer.Write(bytes)
	c.writer.Write([]byte("\n"))
	c.writer.Flush()
}

func (c *Connection) Close() {
	c.connection.Close()
}