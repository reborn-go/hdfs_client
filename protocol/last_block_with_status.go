package protocol

import (
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
)

type LastBlockWithStatus struct {
	LastBlock  LocatedBlockProto
	FileStatus HdfsFileStatusProto
}
