package fs

import (
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	. "github.com/reborn-go/hdfs_client/protocol/protocal_PB"
	"math"
	"fmt"
)

type DFSInputStream struct {
	client         DFSClient
	src            string
	verifyChecksum bool

	// state shared by stateful and positional read:
	// (protected by lock on infoLock)
	////
	locatedBlocks *LocatedBlocksProto
}

func NewDFSInputStream(client DFSClient, src string, verifyChecksum bool) {

}

//Grab the open-file info from namenode
func (in DFSInputStream) openInfo() error {

	return nil
}

func (in DFSInputStream) fetchLocatedBlocksAndGetLastBlockLength() (int64, error) {
	if newInfo, err := in.client.getLocatedBlocks(in.src, 0); err != nil {
		//todo
	} else {
		if in.locatedBlocks != nil {
			locatedBlocks := in.locatedBlocks.GetBlocks()
			newBlocks := newInfo.GetBlocks()
			length := math.Min(float64(len(locatedBlocks)), float64(len(newBlocks)))

			for i := 0; i < int(length); i++ {
				newBlockId := newBlocks[i].GetB().GetBlockId()
				newPoolId := newBlocks[i].GetB().GetPoolId()

				locatedBlockId := locatedBlocks[i].GetB().GetBlockId()
				locatedPoolId := locatedBlocks[i].GetB().GetPoolId()
				if newBlockId != locatedBlockId || newPoolId != locatedPoolId {
					//todo
					return 0, fmt.Errorf("Blocklist for %s has changed! \n", in.src)
				}
			}
		}
		in.locatedBlocks = &newInfo
		lastBlockBeingWrittenLength := 0

		if !newInfo.GetIsLastBlockComplete() {
			last := newInfo.GetLastBlock()
			if last != nil {
				if len(last.GetLocs()) == 0 {
					if *last.GetB().NumBytes == 0 {
						// if the length is zero, then no data has been written to
						// datanode. So no need to wait for the locations.
						return 0, nil
					}
					return -1, nil
				}

			}

		}

	}

}

func (in DFSInputStream) readBlockLength(locatedblock *LocatedBlockProto) (int64, error) {
	replicaNotFoundCount := len(locatedblock.GetLocs())
	//todo  read_timeout
	timeout := 60 * 1000
	nodeList := locatedblock.GetLocs()
	for _, v := range nodeList {
		cdp := NewClientDatanodeProtocolTranslatorPB(*v.Id, int32(timeout), false)
		if n, err := cdp.GetReplicaVisibleLength(locatedblock.GetB()); err != nil {
			//todo
			return 0, err
		} else {
			if n > 0 {
				return n, nil
			}
		}

	}

}

func (in DFSInputStream) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (in DFSInputStream) Close() error {
	return nil
}

func (in DFSInputStream) Flush() error {
	return nil
}
