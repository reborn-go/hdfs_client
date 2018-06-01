package protocol

import (
	. "github.com/reborn-go/hdfs/protocol/hadoop_hdfs"
)

type LastBlockWithStatus struct {
	lastBlock  LocatedBlockProto
	fileStatus HdfsFileStatusProto
}
