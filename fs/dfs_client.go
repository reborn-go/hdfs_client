package fs

import (
	. "github.com/reborn-go/hdfs_client/protocol"
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
)

const (
	prefetchSize = 10 * 128 * 1024 * 1024
)

///********************************************************
// * DFSClient can connect to a Hadoop Filesystem and
// * perform basic file tasks.  It uses the ClientProtocol
// * to communicate with a NameNode daemon, and connects
// * directly to DataNodes to read/write block data.
// ********************************************************/
type DFSClient struct {
	namenode ClientProtocol
}

func (dfs DFSClient) getLocatedBlocks(src string, start int64) (LocatedBlocksProto, error) {
	return dfs.namenode.GetBlockLocations(src, uint64(start), prefetchSize)
}
