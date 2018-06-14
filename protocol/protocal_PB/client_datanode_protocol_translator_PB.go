package protocal_PB

import (
	"github.com/reborn-go/hdfs_client/ipc"
	. "github.com/reborn-go/hdfs_client/protocol"
	"strconv"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_common"
)

type ClientDatanodeProtocolTranslatorPB struct {
	rpcProxy *ipc.Invoker
}

func NewClientDatanodeProtocolTranslatorPB(datanodeId DatanodeIDProto, socketTimeout int32, connectToDnViaHostname bool) ClientDatanodeProtocolTranslatorPB {
	var dnAddr string
	if connectToDnViaHostname {
		dnAddr = datanodeId.GetHostName() + ":" + strconv.Itoa(int(datanodeId.GetIpcPort()))
	} else {
		dnAddr = datanodeId.GetIpAddr() + ":" + strconv.Itoa(int(datanodeId.GetIpcPort()))
	}
	// Since we're creating a new UserGroupInformation here, we know that no
	// future RPC proxies will be able to re-use the same connection. And
	// usages of this proxy tend to be one-off calls.
	//
	// This is a temporary fix: callers should really achieve this by using
	// RPC.stopProxy() on the resulting object, but this is currently not
	// working in trunk. See the discussion on HDFS-1965.
	return ClientDatanodeProtocolTranslatorPB{
		rpcProxy: ipc.NewInvokerDataNode(dnAddr, socketTimeout, 0),
	}
}

func (c ClientDatanodeProtocolTranslatorPB) GetReplicaVisibleLength(b *ExtendedBlockProto) (int64, error) {
	return 0, nil
}

//Refresh the list of federated namenodes from updated configuration
//Adds new namenodes and stops the deleted namenodes.
func (c ClientDatanodeProtocolTranslatorPB) RefreshNamenodes() error {
	return nil
}

//Delete the block pool directory. If force is false it is deleted only if
// it is empty, otherwise it is deleted along with its contents.
func (c ClientDatanodeProtocolTranslatorPB) DeleteBlockPool(bpid string, force bool) error {
	return nil
}

//Retrieves the path names of the block file and metadata file stored on the
//local file system.
//
// In order for this method to work, one of the following should be satisfied:
// The client user must be configured at the datanode to be able to use this
// method.
func (c ClientDatanodeProtocolTranslatorPB) GetBlockLocalPathInfo(block *ExtendedBlockProto, token *TokenProto) (*GetBlockLocalPathInfoResponseProto, error) {
	return nil, nil
}
