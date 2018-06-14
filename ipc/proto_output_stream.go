package ipc

import (
	"github.com/golang/protobuf/proto"
)

type ProtoOutputStream struct {
	TcpConnOutputStream
}

func (out TcpConnOutputStream) WriteP(msgs ...proto.Message) (n int, err error) {
	reqBytes, err := makeRPCPacket(msgs...)
	if err != nil {
		return 0, err
	}
	return out.Write(reqBytes)
}
