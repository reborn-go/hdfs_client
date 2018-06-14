package datatransfer

import (
	. "github.com/reborn-go/hdfs/ipc"
	"fmt"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
)

type BlockReceiver struct {
	in InputStream

	packetReceiver PacketReceiver

	/** the block to receive */
	block ExtendedBlockProto

	inAddr string
	myAddr string

	lastSentTime    int64
	maxSendIdleTime int64
}

func (br BlockReceiver) receivePacket() (int32, error) {
	if err := br.packetReceiver.doRead(br.in); err != nil {
		//todo
		return 0, err
	}

	header := br.packetReceiver.curHeader

	//todo header.getOffsetInBlock() > replicaInfo.getNumBytes()
	if header.GetDataLen() < 0 {
		return 0, fmt.Errorf("Got wrong length during writeBlock( %v ) from %s at offset %d: %d \n", br.block, header.GetOffsetInBlock(), header.GetDataLen())
	}

	//offsetInBlock := header.GetOffsetInBlock()
	seqno := header.GetSeqno()
	lastPacketInBlock := header.GetLastPacketInBlock()
	len := header.GetDataLen()
	syncBlock := header.GetSyncBlock()

	if syncBlock && lastPacketInBlock {
		//todo
	}

	// update received bytes
	//firstByteInBlock := offsetInBlock
	//offsetInBlock += len

	//todo
	//if (replicaInfo.getNumBytes() < offsetInBlock) {

	// put in queue for pending acks, unless sync was requested
	//if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
	//	((PacketResponder) responder.getRunnable()).enqueue(seqno,
	//		lastPacketInBlock, offsetInBlock, Status.SUCCESS);
	//}

	// Drop heartbeat for testing.
	if seqno < 0 && len == 0 {
		return 0, nil
	}

	//dataBuf := br.packetReceiver.curDataSlice
	checksumBuf := br.packetReceiver.curChecksumSlice

	if lastPacketInBlock || len == 0 {
		if syncBlock {
			//todo
		}
	} else {
		checksumBuf.Len()

	}

	return 0, nil

}
