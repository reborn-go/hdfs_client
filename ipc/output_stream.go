package ipc

import (
	"net"
	"io"
)

type OutputStream interface {
	io.WriteCloser
	Flush() error
}

type TcpConnOutputStream struct {
	conn *net.TCPConn
}

func NewTcpConnOutputStream(conn *net.TCPConn) TcpConnOutputStream {
	return TcpConnOutputStream{
		conn: conn,
	}
}

func (out TcpConnOutputStream) Write(p []byte) (n int, err error) {
	return out.conn.Write(p)
}

func (out TcpConnOutputStream) Close() error {
	return out.conn.Close()
}

func (out TcpConnOutputStream) Flush() error {
	return nil
}
