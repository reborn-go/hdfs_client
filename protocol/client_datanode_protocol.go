package protocol

import (
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_common"
)

type ClientDatanodeProtocol interface {
	//Return the visible length of a replica.
	GetReplicaVisibleLength(b *ExtendedBlockProto) (int64, error)

	//Refresh the list of federated namenodes from updated configuration
	//Adds new namenodes and stops the deleted namenodes.
	RefreshNamenodes() error

	//Delete the block pool directory. If force is false it is deleted only if
	// it is empty, otherwise it is deleted along with its contents.
	DeleteBlockPool(bpid string, force bool) error


	//Retrieves the path names of the block file and metadata file stored on the
	//local file system.
	//
	// In order for this method to work, one of the following should be satisfied:
	// The client user must be configured at the datanode to be able to use this
	// method.
	GetBlockLocalPathInfo(block *ExtendedBlockProto,token *TokenProto)(*GetBlockLocalPathInfoResponseProto,error)


}
