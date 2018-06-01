package ipc

import (
	"io"
	"net"
)

type InputStream interface {
	io.ReadCloser //inherit
}

type TcpConnInputStream struct {
	conn *net.TCPConn
}

func NewTcpConnInputStream(conn *net.TCPConn) TcpConnInputStream {
	return TcpConnInputStream{
		conn: conn,
	}

}

func (in TcpConnInputStream) Read(p []byte) (n int, err error) {
	return in.conn.Read(p)
}

func (in TcpConnInputStream) Close() error {
	return in.conn.Close()
}
