package datatransfer

import (
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	"bytes"
	"fmt"
	. "github.com/reborn-go/hdfs_client/ipc"
	"math"
)

type RemoteBlockReader struct {
	filename   string
	peer       DomainPeer
	datanodeId DatanodeIDProto
	blcokId    int64

	curDataSlice bytes.Buffer

	packetReceiver PacketReceiver
	in             InputStream
	out            TcpConnOutputStream

	// The total number of bytes we need to transfer from the DN.
	// This is the amount that the user has requested plus some padding
	// at the beginning so that the read can begin on a chunk boundary.
	bytesNeededToFinish int64

	//offset in block where reader wants to actually read
	startOffset int64

	checksum         *DataChecksum
	verifyChecksum   bool
	sentStatusCode   bool
	bytesPerChecksum int32
	checksumSize     int32
	lastSeqNo        int64
}

func NewRemoteBlockReader(file string, blockId int64, checksum *DataChecksum, verifyChecksum bool, startOffset int64,
	firstChunkOffset int64, bytesToRead int64, peer DomainPeer, datanodeId DatanodeIDProto) RemoteBlockReader {
	return RemoteBlockReader{
		peer:           peer,
		datanodeId:     datanodeId,
		blcokId:        blockId,
		in:             peer.in,
		out:            peer.out,
		checksum:       checksum,
		verifyChecksum: verifyChecksum,
		filename:       file,
		startOffset:    int64(math.Max(float64(startOffset), 0)),

		// The total number of bytes that we need to transfer from the DN is
		// the amount that the user wants (bytesToRead), plus the padding at
		// the beginning in order to chunk-align. Note that the DN may elect
		// to send more than this amount if the read starts/ends mid-chunk.
		bytesNeededToFinish: bytesToRead + (startOffset - firstChunkOffset),
		bytesPerChecksum:    int32(checksum.ChunkSize),
		checksumSize:        checksum.Size,
	}

}

func (rbr RemoteBlockReader) read(buf []byte) (n int, err error) {
	if rbr.curDataSlice.Len() == 0 {
		err := rbr.readNextPacket()
		if err != nil {
			//todo
		}
	}
	if rbr.curDataSlice.Len() == 0 {
		//todo
		return -1, nil
	}

	n, err = rbr.curDataSlice.Read(buf)
	return
}

//todo need?
func (rbr RemoteBlockReader) skip(n int64) (err error) {
	return nil
}

func (rbr RemoteBlockReader) readNextPacket() error {
	if err := rbr.packetReceiver.receiveNextPacket(rbr.in); err != nil {
		//todo
		return err
	}

	curHeader := rbr.packetReceiver.curHeader
	rbr.curDataSlice = rbr.packetReceiver.curDataSlice

	if int32(rbr.curDataSlice.Len()) != curHeader.GetDataLen() {
		return fmt.Errorf("curDataSlice.Len %d is inconsistent with header.GetDataLen %d \n", rbr.curDataSlice.Len(), curHeader.GetDataLen())
	}

	if curHeader.GetDataLen() > 0 {

		chunks := 1 + (curHeader.GetDataLen()-1)/rbr.bytesPerChecksum
		checksumsLen := chunks * rbr.checksumSize

		if int32(rbr.packetReceiver.curChecksumSlice.Len()) != checksumsLen {
			return fmt.Errorf("checksum slice capacity=%d checksumsLen= %d \n", rbr.packetReceiver.curChecksumSlice.Len(), checksumsLen)
		}

		//todo checksum
		if rbr.curDataSlice.Len() > 0 {
			rbr.lastSeqNo = curHeader.GetSeqno()
			if rbr.verifyChecksum {
				if err := rbr.checksum.VerifyChunkedSums(rbr.curDataSlice, rbr.packetReceiver.curChecksumSlice); err != nil {
					return err
				}
			}

		}

		rbr.bytesNeededToFinish -= int64(curHeader.GetDataLen())
	}

	// First packet will include some data prior to the first byte
	// the user requested. Skip it.
	if curHeader.GetOffsetInBlock() < rbr.startOffset {
		//todo log
		newPos := rbr.startOffset - curHeader.GetOffsetInBlock()
		rbr.curDataSlice.Next(int(newPos))
	}

	// If we've now satisfied the whole client read, read one last packet
	// header, which should be empty
	if rbr.bytesNeededToFinish <= 0 {
		rbr.readTrailingEmptyPacket()
		if rbr.verifyChecksum {
			rbr.sendReadResult(Status_CHECKSUM_OK)
		} else {
			rbr.sendReadResult(Status_SUCCESS)
		}
	}

	return nil

}

func (rbr RemoteBlockReader) readTrailingEmptyPacket() error {
	if err := rbr.packetReceiver.receiveNextPacket(rbr.in); err != nil {
		//todo
		return err
	}
	trailer := rbr.packetReceiver.curHeader

	if !trailer.GetLastPacketInBlock() || trailer.GetDataLen() != 0 {
		return fmt.Errorf("Expected empty end-of-read packet! Header: %v \n", trailer)
	}
	return nil
}

// When the reader reaches end of the read, it sends a status response
// (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
// closing our connection (which we will re-open), but won't affect
// data correctness.
func (rbr RemoteBlockReader) sendReadResult(statusCode Status) (err error) {
	if rbr.sentStatusCode {
		return fmt.Errorf("already sent status code")
	}
	err = writeReadResult(rbr.out, statusCode)
	return
}

func writeReadResult(out TcpConnOutputStream, statusCode Status) (err error) {
	rsq := &ClientReadStatusProto{
		Status: &statusCode,
	}
	_, err = out.WriteP(rsq)
	return
}
