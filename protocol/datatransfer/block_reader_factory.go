package datatransfer

import (
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	"time"
	"github.com/golang/protobuf/proto"
	. "github.com/reborn-go/hdfs_client/ipc"
	"github.com/reborn-go/hdfs_client/protocol/hadoop_common"
	"fmt"
)

type BlockReaderFactory struct {
	//The file name, for logging and debugging purposes.
	fileName string
	//The block ID and block pool ID to use.
	block ExtendedBlockProto

	blockToken hadoop_common.TokenProto
	//The offset within the block to start reading at.
	startOffset int64
	//If false, we won't try to verify the block checksum.
	verifyChecksum bool
	//The name of this client.
	clientName string
	//The DataNode we're talking to.
	datanode DatanodeInfoProto
	//StorageType of replica on DataNode.
	storageType StorageTypeProto
	//Number of bytes to read.  -1 indicates no limit.
	length int64

	address string
}


//
//Get a RemoteBlockReader that communicates over a TCP socket.
//
//todo should share the same peer when connecting the same datanode
func (brf *BlockReaderFactory) getRemoteBlockReaderFromTcp() (BlockReader, error) {
	if peer, err := brf.nextTcpPeer(); err != nil {
		return nil, err
	} else {
		blockReader, err := brf.newBlockReader(*peer)
		return blockReader, err
	}
}

//
// Get the next TCP-based peer-- either from the cache or by creating it.
//
func (brf *BlockReaderFactory) nextTcpPeer() (*DomainPeer, error) {
	if peer, err := NewDomainPeer(brf.address, 5*time.Second); err != nil {
		//todo
		return nil, err
	} else {
		return peer, nil
	}

}

// Create a new BlockReader specifically to satisfy a read.
// This method also sends the OP_READ_BLOCK request.
func (brf *BlockReaderFactory) newBlockReader(peer DomainPeer) (BlockReader, error) {

	// A read request to a datanode:
	// +-----------------------------------------------------------+
	// |  Data Transfer Protocol Version, int16                    |
	// +-----------------------------------------------------------+
	// |  Op code, 1 byte (READ_BLOCK = 0x51)                      |
	// +-----------------------------------------------------------+
	// |  varint length + OpReadBlockProto                         |
	// +-----------------------------------------------------------+

	proto := &OpReadBlockProto{
		Header: &ClientOperationHeaderProto{
			BaseHeader: &BaseHeaderProto{
				Block: &brf.block,
				Token: &brf.blockToken,
			},
			ClientName: proto.String(brf.clientName),
		},
		Offset:        proto.Uint64(uint64(brf.startOffset)),
		Len:           proto.Uint64(uint64(brf.length)),
		SendChecksums: proto.Bool(brf.verifyChecksum),
	}
	if err := WriteBlockOpRequest(peer.out, ReadBlockOp, proto); err != nil {
		//todo
		return nil, err
	} else {
		if status, err := ReadBlockOpResponse(peer.in); err != nil {
			//todo
			return nil, err
		} else {
			if *status.Status != Status_SUCCESS {
				if *status.Status == Status_ERROR_ACCESS_TOKEN {
					return nil, fmt.Errorf("Got access token error,status message %s \n", status.GetMessage())
				} else {
					return nil, fmt.Errorf("Got error,status message %s \n", status.GetMessage())
				}
			} else {
				checksumInfo := status.GetReadOpChecksumInfo()
				checksum := checksumInfo.GetChecksum()

				firstChunkOffset := int64(checksumInfo.GetChunkOffset())

				if firstChunkOffset < 0 || firstChunkOffset > brf.startOffset || firstChunkOffset <= (brf.startOffset-int64(checksum.GetBytesPerChecksum())) {
					//todo
					return nil, fmt.Errorf("BlockReader: error in first chunk offset ( %d ) startOffset is %d for file %s\n", firstChunkOffset, brf.startOffset, brf.fileName)
				}

				if dataChecksum, err := NewDataChecksum(checksum); err != nil {
					//todo
					return nil, fmt.Errorf("BlockReader: error in NewDataChecksum  err is %s \n", err)
				} else {
					blockReader := NewRemoteBlockReader(brf.fileName, int64(brf.block.GetBlockId()), dataChecksum, brf.verifyChecksum,
						brf.startOffset, firstChunkOffset, brf.length, peer, *brf.datanode.GetId())
					return blockReader, nil
				}
			}

		}

	}

}
