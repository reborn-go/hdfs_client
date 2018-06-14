package datatransfer

import (
	. "github.com/reborn-go/hdfs_client/ipc"
	"net"
	"fmt"
	"time"
)

type DomainPeer struct {
	in      InputStream
	out     TcpConnOutputStream
	address string
}

func NewDomainPeer(address string, connectionTimeout time.Duration) (*DomainPeer, error) {
	dp := DomainPeer{
		address: address,
	}
	if err := dp.setupIOstreams(connectionTimeout); err != nil {
		return nil, err
	}
	return &dp, nil
}

func setupConnection(address string, connectionTimeout time.Duration) (*net.TCPConn, error) {
	conn, err := net.DialTimeout("tcp", address, connectionTimeout)
	if err != nil {
		//todo
		fmt.Println(err)
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetNoDelay(true)
	return tcpConn, nil
}

func (dp *DomainPeer) setupIOstreams(connectionTimeout time.Duration) error {
	con, err := setupConnection(dp.address, connectionTimeout)
	if err != nil {
		//todo
	}
	dp.in = NewTcpConnInputStream(con)
	dp.out = NewTcpConnOutputStream(con)
	return nil
}
