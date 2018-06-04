package ipc

import (
	"github.com/golang/protobuf/proto"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_common"
	"go.uber.org/atomic"
)

const (
	protocolClass        = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
	protocolClassVersion = 1
)

var (
	client = newClient()
)

type Invoker struct {
	returnTypes           map[string]proto.Message
	isClosed              bool
	remoteId              ConnectionId
	client                *Client
	clientProtocolVersion int64
	protocolName          string
	fallbackToSimpleAuth  atomic.Bool
}

func NewInvoker(address string, rpcTimeout int32) *Invoker {
	return &Invoker{
		protocolName:          protocolClass,
		clientProtocolVersion: protocolClassVersion,
		client:                client,
		remoteId:              newConnectionId(address, rpcTimeout),
	}
}

func (i *Invoker) constructRpcRequestHeader(methodName string) *RequestHeaderProto {
	return &RequestHeaderProto{
		MethodName:                 proto.String(methodName),
		DeclaringClassProtocolName: proto.String(protocolClass),
		ClientProtocolVersion:      proto.Uint64(uint64(protocolClassVersion)),
	}
}

func (i *Invoker) Invoke(methodName string, theRequest proto.Message, rsqI interface{}) (err error) {
	theResponse := rsqI.(proto.Message)
	rsq := &theResponse
	rpcRequestHeader := i.constructRpcRequestHeader(methodName)
	rpcKind := RpcKindProto_RPC_PROTOCOL_BUFFER.Enum()
	err = i.client.call(*rpcKind,
		RpcRequestWrapper{rpcRequestHeader, theRequest}, rsq, i.remoteId, i.fallbackToSimpleAuth)
	return
}
