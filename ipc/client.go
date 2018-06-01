package ipc

import (
	"time"
	"github.com/golang/protobuf/proto"
	"sync"
	"net"
	. "github.com/colinmarc/hdfs/protocol/hadoop_common"
	"os/user"
	"go.uber.org/atomic"
	"fmt"
	"encoding/binary"
	"io"
)

const (
	CURRENT_VERSION = byte(9)
	AUTH_PROTOCOL   = byte(0)
	handshakeCallID = -3
)

var (
	PING_CALL_ID        = int32(-4)
	INVALID_RETRY_COUNT = int32(-1)
	mutex               = sync.Mutex{}
	touchLocker         = sync.Mutex{}
	callIdCounter       = atomic.NewUint32(0)
)

type Client struct {
	connectionTimeout time.Duration
	fallbackAllowed   bool
	clientId          []byte
	connections       map[ConnectionId]*Connection
}

func newClient() *Client {
	return &Client{
		clientId:          getClientId(),
		connectionTimeout: 20 * time.Second,
		fallbackAllowed:   false,
		connections:       make(map[ConnectionId]*Connection),
	}
}

func (c *Client) call(rpcKind RpcKindProto, rpcRequest RpcRequestWrapper, theResponse *proto.Message,
	remoteId ConnectionId, fallbackToSimpleAuth atomic.Bool) (rsq proto.Message, err error) {
	call := createCall(rpcKind, rpcRequest, theResponse)
	connection, err := c.getConnection(remoteId, call, 0, fallbackToSimpleAuth)
	if err != nil {
		//todo
	} else {
		connection.addCall(*call)
	}
	<-call.rspChan
	return
	//todo
}

func (c *Client) getConnection(remoteId ConnectionId, call *Call, serviceClass int32, fallbackToSimpleAuth atomic.Bool) (connection *Connection, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	connection, ok := c.connections[remoteId]
	if !ok {
		connection = NewConnection(remoteId, serviceClass, c.clientId)
		connection.setupIOstreams()
		connection.run()
	} else {
		connection.calls[call.Id] = *call
	}
	return
}

type Connection struct {
	address                    string
	remoteId                   ConnectionId
	serviceClass               int32
	rpcTimeout                 int32
	maxIdleTime                int64
	maxRetriesOnSocketTimeouts int32
	tcpNoDelay                 bool
	doPing                     bool
	pingInterval               int64
	calls                      map[uint32]Call
	lastActivity               int64
	shouldCloseConnection      bool
	callsChan                  chan Call
	pingChan                   chan proto.Message
	connectionTimeout          time.Duration
	conn                       *net.TCPConn
	in                         InputStream
	out                        OutputStream
	clientId                   []byte
}

func NewConnection(remoteId ConnectionId, serviceClass int32, clientId []byte) (c *Connection) {
	c = &Connection{
		address:                    remoteId.address,
		remoteId:                   remoteId,
		serviceClass:               serviceClass,
		rpcTimeout:                 remoteId.rpcTimeout,
		maxIdleTime:                remoteId.maxIdleTime,
		maxRetriesOnSocketTimeouts: remoteId.maxRetriesOnSocketTimeouts,
		tcpNoDelay:                 remoteId.tcpNoDelay,
		doPing:                     remoteId.doPing,
		pingInterval:               remoteId.pingInterval,
		calls:                      make(map[uint32]Call),
		lastActivity:               time.Now().Unix(),
		shouldCloseConnection:      false,
		callsChan:                  make(chan Call, 100),
		pingChan:                   make(chan proto.Message),
		connectionTimeout:          20 * time.Second,
		clientId:                   clientId,
	}
	return

}
func (c *Connection) run() {
	//goroutine
	go c.call()
	go c.sendRpcRequests()
	go c.sendPing()
}

func (c *Connection) touch() {
	touchLocker.Lock()
	defer touchLocker.Unlock()
	c.lastActivity = time.Now().Unix()
}

func (c *Connection) addCall(call Call) bool {
	if c.shouldCloseConnection {
		return false
	}
	fmt.Print("add call. id is : ")
	fmt.Println(call.Id)
	c.calls[call.Id] = call
	c.callsChan <- call
	return true
}

func (c *Connection) waitForWork() bool {
	if len(c.calls) == 0 && !c.shouldCloseConnection {
		timeout := c.maxIdleTime - (time.Now().Unix() - c.lastActivity)
		if timeout > 0 {
			timeout = 1
			t := time.NewTimer(time.Duration(timeout) * time.Second)
			<-t.C
		}
	}
	if len(c.calls) != 0 && !c.shouldCloseConnection {
		return true
	} else if c.shouldCloseConnection {
		return false
	}
	return false
}

func (c *Connection) call() {
	for {
		if c.waitForWork() {
			fmt.Println("call enter")
			c.receiveRpcResponse()
		}
	}
}

func (c *Connection) receiveRpcResponse() {
	if c.shouldCloseConnection {
		return
	}
	c.readResponse()
	c.touch()
}

func (c *Connection) setupIOstreams() {
	err := c.setupConnection()
	if err != nil {
		//todo
	}
	c.in = NewTcpConnInputStream(c.conn)
	c.out = NewTcpConnOutputStream(c.conn)
	c.writeConnectionHeader()
	c.writeConnectionContext()
	c.touch()
}

func (c *Connection) sendPing() error {
	for {
		//todo add stop
		timeout := c.pingInterval - (time.Now().Unix() - c.lastActivity)
		if timeout > 0 {
			t := time.NewTimer(time.Duration(timeout) * time.Second)
			<-t.C
		}
		pingRequest := RpcRequestHeaderProto{
			RpcKind:    RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
			RpcOp:      RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
			CallId:     &PING_CALL_ID,
			RetryCount: &INVALID_RETRY_COUNT,
			ClientId:   c.clientId,
		}
		c.pingChan <- &pingRequest
		c.touch()
	}
	return nil
}

func (c *Connection) sendRpcRequests() (err error) {
	//for call := range c.callsChan {
	//	fmt.Print(call)
	//	err = c.writeRequest(call)
	//}

	for {
		select {
		case call := <-c.callsChan:
			fmt.Print("send ")
			fmt.Println(call)
			err = c.writeRequest(call)
		default:
			fmt.Println("ping request sended")
			ping := <-c.pingChan
			reqBytes, err := makeRPCPacket(ping)
			if err != nil {
				return err
			}
			_, err = c.out.Write(reqBytes)

		}
	}
	return
}

func (c *Connection) setupConnection() error {
	conn, err := net.DialTimeout("tcp", c.address, c.connectionTimeout)
	if err != nil {
		//todo
		fmt.Println(err)
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetNoDelay(c.tcpNoDelay)
	c.conn = tcpConn
	return nil
}

//
//Write the connection header - this is sent when connection is established
//   +----------------------------------+
//   |  "hrpc" 4 bytes                  |
//   +----------------------------------+
//   |  Version (1 byte)                |
//   +----------------------------------+
//   |  Service Class (1 byte)          |
//   +----------------------------------+
//   |  AuthProtocol (1 byte)           |
//   +----------------------------------+
//
func (c *Connection) writeConnectionHeader() {
	rpcHeader := []byte("hrpc")
	rpcHeader = append(rpcHeader, CURRENT_VERSION)
	rpcHeader = append(rpcHeader, byte(c.serviceClass))
	rpcHeader = append(rpcHeader, AUTH_PROTOCOL)
	c.out.Write(rpcHeader)
}

func (c *Connection) writeConnectionContext() error {
	user, _ := user.Current()
	rrh := newRPCRequestHeader(handshakeCallID, c.clientId)
	cc := newConnectionContext(user.Username)
	packet, err := makeRPCPacket(rrh, cc)
	if err != nil {
		return err
	}
	c.out.Write(packet)
	return nil
}

// A request packet:
// +-----------------------------------------------------------+
// |  uint32 length of the next three parts                    |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + RequestHeaderProto                       |
// +-----------------------------------------------------------+
// |  varint length + Request                                  |
// +-----------------------------------------------------------+
func (c *Connection) writeRequest(call Call) error {
	rrh := newRPCRequestHeader(int32(call.Id), c.clientId)
	reqBytes, err := makeRPCPacket(rrh, call.RpcRequest.requestHeader, call.RpcRequest.theRequest)
	if err != nil {
		return err
	}

	_, err = c.out.Write(reqBytes)
	return err
}

// A response from the namenode:
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcResponseHeaderProto                   |
// +-----------------------------------------------------------+
// |  varint length + Response                                 |
// +-----------------------------------------------------------+
func (c *Connection) readResponse() error {
	var packetLength uint32
	err := binary.Read(c.in, binary.BigEndian, &packetLength)
	if err != nil {
		return err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(c.conn, packet)
	if err != nil {
		return err
	}

	rrh := &RpcResponseHeaderProto{}
	err = readRPCPacket(packet, rrh)
	callId := rrh.CallId
	call, ok := c.calls[*callId]
	if !ok {
		//todo
	}
	fmt.Println("")
	fmt.Print("receiced call  Id is: ")
	fmt.Println(*callId)
	resp := call.RpcResponse
	//resp := &GetFileInfoResponseProto{}
	err = readRPCPacket(packet, rrh, *resp)
	if err != nil {
		//todo
	}
	//fmt.Println(rpcResponse)
	delete(c.calls, *callId)
	call.rspChan <- struct{}{}
	return nil
}

func newRPCRequestHeader(id int32, clientID []byte) *RpcRequestHeaderProto {
	return &RpcRequestHeaderProto{
		RpcKind:  RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(id),
		ClientId: clientID,
	}
}

func newConnectionContext(user string) *IpcConnectionContextProto {
	return &IpcConnectionContextProto{
		UserInfo: &UserInformationProto{
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protocolClass),
	}
}

type CallBase interface {
}

type Call struct {
	Id          uint32
	RpcKind     RpcKindProto
	RpcRequest  RpcRequestWrapper
	rspChan     chan struct{}
	RpcResponse *proto.Message
}

func createCall(RpcKind RpcKindProto, RpcRequest RpcRequestWrapper, theResponse *proto.Message) *Call {
	return &Call{
		Id:          callIdCounter.Inc(),
		RpcKind:     RpcKind,
		RpcRequest:  RpcRequest,
		rspChan:     make(chan struct{}),
		RpcResponse: theResponse,
	}
}

type ConnectionId struct {
	address                    string
	rpcTimeout                 int32
	maxIdleTime                int64 //connections will be culled if it was idle for
	maxRetriesOnSocketTimeouts int32
	tcpNoDelay                 bool  // if T then disable Nagle's Algorithm
	doPing                     bool  //do we need to send ping message
	pingInterval               int64 // how often sends ping to the server in msecs
}

func newConnectionId(address string, rpcTimeout int32) ConnectionId {
	return ConnectionId{
		address:                    address,
		rpcTimeout:                 rpcTimeout,
		maxIdleTime:                10,
		maxRetriesOnSocketTimeouts: 45,
		tcpNoDelay:                 true,
		doPing:                     true,
		pingInterval:               2,
	}
}

type RpcRequestWrapper struct {
	requestHeader proto.Message
	theRequest    proto.Message
}
