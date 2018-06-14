package ipc

import (
	"github.com/rs/xid"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	"hash/crc32"
	"fmt"
	"bytes"
	"encoding/binary"
)

func getClientId() []byte {
	guid := xid.New()
	return guid.Bytes()
}

//type Checksum interface {
//	update(b int)
//	update(b []byte,off int, len int)
//	getValue() int64
//	reset()
//}

type DataChecksum struct {
	checksumType ChecksumTypeProto
	checksumTab  *crc32.Table
	ChunkSize    int
	Size         int32
}

//func NewDataChecksum1(checksumType ChecksumTypeProto, bytesPerChecksum int32) (*DataChecksum, error) {
//	var checksumTab *crc32.Table
//	switch checksumType {
//	case ChecksumTypeProto_CHECKSUM_CRC32:
//		checksumTab = crc32.IEEETable
//	case ChecksumTypeProto_CHECKSUM_CRC32C:
//		checksumTab = crc32.MakeTable(crc32.Castagnoli)
//	default:
//		return nil, fmt.Errorf("Unsupported checksum type: %d \n", checksumType)
//	}
//	return &DataChecksum{
//		checksumType: checksumType,
//		checksumTab:  checksumTab,
//		ChunkSize:    bytesPerChecksum,
//		Size:         4,
//	}, nil
//}

func NewDataChecksum(checksumInfo *ChecksumProto) (*DataChecksum, error) {
	var checksumTab *crc32.Table
	checksumType := checksumInfo.GetType()
	switch checksumType {
	case ChecksumTypeProto_CHECKSUM_CRC32:
		checksumTab = crc32.IEEETable
	case ChecksumTypeProto_CHECKSUM_CRC32C:
		checksumTab = crc32.MakeTable(crc32.Castagnoli)
	default:
		return nil, fmt.Errorf("Unsupported checksum type: %d \n", checksumType)
	}
	return &DataChecksum{
		checksumType: checksumType,
		checksumTab:  checksumTab,
		ChunkSize:    int(checksumInfo.GetBytesPerChecksum()),
		Size:         4,
	}, nil
}

func (dc *DataChecksum) VerifyChunkedSums(data bytes.Buffer, checksums bytes.Buffer) error {
	for data.Len() > 0 {
		checksum := binary.BigEndian.Uint32(checksums.Next(4))
		chunkBytes := data.Next(dc.ChunkSize)
		crc := crc32.Checksum(chunkBytes, dc.checksumTab)
		if crc != checksum {
			return fmt.Errorf("Invalid checksum \n")
		}
	}
	return nil
}
