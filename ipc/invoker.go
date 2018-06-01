package ipc

import (
	"github.com/golang/protobuf/proto"
	. "github.com/colinmarc/hdfs/protocol/hadoop_common"
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

func (i *Invoker) Invoke(methodName string, theRequest proto.Message, rsqI interface{}) {
	theResponse := rsqI.(proto.Message)
	rpcRequestHeader := i.constructRpcRequestHeader(methodName)
	rpcKind := RpcKindProto_RPC_PROTOCOL_BUFFER.Enum()
	i.client.call(*rpcKind,
		RpcRequestWrapper{rpcRequestHeader, theRequest},&theResponse, i.remoteId, i.fallbackToSimpleAuth)

}
