package protocal_PB

import (
	. "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"fmt"
)

type ClientNamenodeProtocolTranslatorPB struct {
}

func (c *ClientNamenodeProtocolTranslatorPB) getBlockLocations(src string, offset uint64, length uint64) {
	req := &GetBlockLocationsRequestProto{
		Src:    &src,
		Offset: &offset,
		Length: &length,
	}
	fmt.Println(req)
}
