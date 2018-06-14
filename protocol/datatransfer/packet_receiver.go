package datatransfer

import (
	"bytes"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	. "github.com/reborn-go/hdfs_client/ipc"
	"io"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	//len(int32) + len(int16)
	PKT_LENGTHS_LEN = 6
)

type PacketReceiver struct {
	//A slice of curPacketBuf which contains just the checksums.
	curChecksumSlice bytes.Buffer
	//A slice of curPacketBuf which contains just the data.
	curDataSlice bytes.Buffer

	curHeader PacketHeaderProto
}

func (pr PacketReceiver) receiveNextPacket(in InputStream) error {
	if err := pr.doRead(in); err != nil {
		return err
	}
	return nil
}

// Each packet looks like:
//   PLEN    HLEN      HEADER     CHECKSUMS  DATA
//   32-bit  16-bit   <protobuf>  <variable length>
//
// PLEN:      Payload length
//            = length(PLEN) + length(CHECKSUMS) + length(DATA)
//            This length includes its own encoded length in
//            the sum for historical reasons.
//
// HLEN:      Header length
//            = length(HEADER)
//
// HEADER:    the actual packet header fields, encoded in protobuf
// CHECKSUMS: the crcs for the data chunk. May be missing if
//            checksums were not requested
// DATA       the actual block data
func (pr PacketReceiver) doRead(in InputStream) error {
	lengthBytes := make([]byte, PKT_LENGTHS_LEN)
	_, err := io.ReadFull(in, lengthBytes)
	if err != nil {
		return err
	}
	payloadLen := binary.BigEndian.Uint32(lengthBytes[:4])
	if payloadLen < 4 {
		return fmt.Errorf("Invalid payload length %d \n", payloadLen)
	}
	dataPlusChecksumLen := int32(payloadLen) - 4
	headerLen := binary.BigEndian.Uint16(lengthBytes[4:])
	//todo log
	fmt.Printf("readNextPacket: dataPlusChecksumLen = %d  headerLen = %d \n", dataPlusChecksumLen, headerLen)

	headerBytes := make([]byte, headerLen)
	_, err = io.ReadFull(in, headerBytes)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(headerBytes, &pr.curHeader); err != nil {
		return err
	}

	checksumLen := dataPlusChecksumLen - pr.curHeader.GetDataLen()
	if checksumLen < 0 {
		return fmt.Errorf("Invalid packet: data length in packet header exceeds data length received. dataPlusChecksumLen=%d header: %v \n ", dataPlusChecksumLen, pr.curHeader)
	}
	if checksumLen != 0 {
		checksumBytes := make([]byte, checksumLen)
		_, err = io.ReadFull(in, checksumBytes)
		if err != nil {
			return err
		}
		pr.curChecksumSlice.Write(checksumBytes)
	}

	dataBytes := make([]byte, pr.curHeader.GetDataLen())
	_, err = io.ReadFull(in, dataBytes)
	if err != nil {
		return err
	}
	pr.curDataSlice.Write(dataBytes)
	return nil
}
