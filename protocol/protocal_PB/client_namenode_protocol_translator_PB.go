package protocal_PB

import (
	. "github.com/reborn-go/hdfs_client/protocol/hadoop_hdfs"
	"github.com/reborn-go/hdfs_client/ipc"
	. "github.com/reborn-go/hdfs_client/protocol"
	"github.com/golang/protobuf/proto"
	"fmt"
)

const (
	getBlockLocations     = "getBlockLocations"
	getServerDefaults     = "getServerDefaults"
	create                = "create"
	append                = "append"
	setReplication        = "setReplication"
	setPermission         = "setPermission"
	setOwner              = "setOwner"
	abandonBlock          = "abandonBlock"
	addBlock              = "addBlock"
	getAdditionalDatanode = "getAdditionalDatanode"
	complete              = "complete"
	reportBadBlocks       = "reportBadBlocks"
	rename                = "rename"
	concat                = "concat"
	truncate              = "truncate"
	delete                = "delete"
	mkdirs                = "mkdirs"
	renewLease            = "renewLease"
	recoverLease          = "recoverLease"
)

type ClientNamenodeProtocolTranslatorPB struct {
	rpcProxy *ipc.Invoker
}

//for test
func New() ClientNamenodeProtocolTranslatorPB {
	return ClientNamenodeProtocolTranslatorPB{
		rpcProxy: ipc.NewInvoker("hadoop-infra-0.recommend.shopeemobile.com:8020", int32(10)),
	}

}

func (c ClientNamenodeProtocolTranslatorPB) GetBlockLocations(src string, offset uint64, length uint64) (LocatedBlocksProto, error) {
	req := &GetBlockLocationsRequestProto{
		Src:    proto.String(src),
		Offset: proto.Uint64(offset),
		Length: proto.Uint64(length),
	}
	rsq := &GetBlockLocationsResponseProto{}
	err := c.rpcProxy.Invoke(getBlockLocations, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return *rsq.GetLocations(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) GetServerDefaults() (FsServerDefaultsProto, error) {
	req := &GetServerDefaultsRequestProto{}
	rsq := &GetServerDefaultsResponseProto{}

	err := c.rpcProxy.Invoke(getServerDefaults, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return *rsq.GetServerDefaults(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) Create(src string, masked FsPermissionProto, clientName string, flag []CreateFlagProto,
	createParent bool, replication uint32,
	blockSize uint64, supportedVersions []CryptoProtocolVersionProto) (HdfsFileStatusProto, error) {
	req := &CreateRequestProto{
		Src:                   proto.String(src),
		Masked:                &masked,
		ClientName:            proto.String(clientName),
		CreateFlag:            proto.Uint32(convertCreateFlag(flag)),
		CreateParent:          proto.Bool(createParent),
		Replication:           proto.Uint32(replication),
		BlockSize:             proto.Uint64(blockSize),
		CryptoProtocolVersion: supportedVersions,
	}
	rsq := &CreateResponseProto{}

	err := c.rpcProxy.Invoke(create, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return *rsq.GetFs(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) Append(src string, clientName string, flag []CreateFlagProto) (LastBlockWithStatus, error) {
	req := &AppendRequestProto{
		Src:        proto.String(src),
		ClientName: proto.String(clientName),
		Flag:       proto.Uint32(convertCreateFlag(flag)),
	}
	rsq := &AppendResponseProto{}
	err := c.rpcProxy.Invoke(append, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	lastBlockWithStatus := LastBlockWithStatus{
		LastBlock:  *rsq.GetBlock(),
		FileStatus: *rsq.GetStat(),
	}
	return lastBlockWithStatus, nil
}
func (c ClientNamenodeProtocolTranslatorPB) SetReplication(src string, replication uint32) (bool, error) {
	req := &SetReplicationRequestProto{
		Src:         proto.String(src),
		Replication: proto.Uint32(replication),
	}
	rsq := &SetReplicationResponseProto{}
	err := c.rpcProxy.Invoke(setReplication, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) SetPermission(src string, permission FsPermissionProto) error {
	req := &SetPermissionRequestProto{
		Src:        proto.String(src),
		Permission: &permission,
	}
	rsq := &SetPermissionResponseProto{}
	err := c.rpcProxy.Invoke(setPermission, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return nil
}

func (c ClientNamenodeProtocolTranslatorPB) SetOwner(src string, username string, groupname string) error {
	req := &SetOwnerRequestProto{
		Src:       proto.String(src),
		Username:  proto.String(username),
		Groupname: proto.String(groupname),
	}
	rsq := &SetOwnerResponseProto{}
	err := c.rpcProxy.Invoke(setOwner, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return nil
}

func (c ClientNamenodeProtocolTranslatorPB) AbandonBlock(b ExtendedBlockProto, fileId uint64, src string, holder string) error {
	req := &AbandonBlockRequestProto{
		Src:    proto.String(src),
		Holder: proto.String(holder),
		FileId: proto.Uint64(fileId),
		B:      &b,
	}
	rsq := &AbandonBlockResponseProto{}
	err := c.rpcProxy.Invoke(abandonBlock, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return nil
}

func (c ClientNamenodeProtocolTranslatorPB) AddBlock(src string, clientName string, previous ExtendedBlockProto,
	excludeNodes []*DatanodeInfoProto, fileId uint64, favoredNodes []string) (LocatedBlockProto, error) {
	req := &AddBlockRequestProto{
		Src:          proto.String(src),
		ClientName:   proto.String(clientName),
		Previous:     &previous,
		ExcludeNodes: excludeNodes,
		FavoredNodes: favoredNodes,
		FileId:       proto.Uint64(fileId),
	}
	rsq := &AddBlockResponseProto{}
	err := c.rpcProxy.Invoke(addBlock, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return *rsq.GetBlock(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) GetAdditionalDatanode(src string, fileId uint64, blk ExtendedBlockProto, existings []*DatanodeInfoProto,
	existingStorageIDs []string, excludes []*DatanodeInfoProto,
	numAdditionalNodes uint32, clientName string) (LocatedBlockProto, error) {
	req := &GetAdditionalDatanodeRequestProto{
		Src:                  proto.String(src),
		FileId:               proto.Uint64(fileId),
		Blk:                  &blk,
		Existings:            existings,
		ExistingStorageUuids: existingStorageIDs,
		Excludes:             excludes,
		NumAdditionalNodes:   proto.Uint32(numAdditionalNodes),
		ClientName:           proto.String(clientName),
	}
	rsq := &GetAdditionalDatanodeResponseProto{}
	err := c.rpcProxy.Invoke(getAdditionalDatanode, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return *rsq.GetBlock(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) Complete(src string, clientName string, last ExtendedBlockProto, fileId uint64) (bool, error) {
	req := &CompleteRequestProto{
		Src:        proto.String(src),
		ClientName: proto.String(clientName),
		FileId:     proto.Uint64(fileId),
	}
	rsq := &CompleteResponseProto{}

	err := c.rpcProxy.Invoke(complete, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) ReportBadBlocks(blocks []*LocatedBlockProto) error {
	req := &ReportBadBlocksRequestProto{
		Blocks: blocks,
	}
	rsq := &ReportBadBlocksResponseProto{}
	err := c.rpcProxy.Invoke(reportBadBlocks, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return nil
}

func (c ClientNamenodeProtocolTranslatorPB) Rename(src string, dst string) (bool, error) {
	req := &RenameRequestProto{
		Src: proto.String(src),
		Dst: proto.String(dst),
	}

	rsq := &RenameResponseProto{}

	err := c.rpcProxy.Invoke(rename, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) Concat(src string, srcs []string) error {
	req := &ConcatRequestProto{
		Trg:  proto.String(src),
		Srcs: srcs,
	}

	rsq := &ConcatResponseProto{}

	err := c.rpcProxy.Invoke(concat, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return nil
}

func (c ClientNamenodeProtocolTranslatorPB) Truncate(src string, newLength uint64, clientName string) (bool, error) {
	req := &TruncateRequestProto{
		Src:        proto.String(src),
		NewLength:  proto.Uint64(newLength),
		ClientName: proto.String(clientName),
	}
	rsq := &TruncateResponseProto{}

	err := c.rpcProxy.Invoke(truncate, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) Delete(src string, recursive bool) (bool, error) {
	req := &DeleteRequestProto{
		Src:       proto.String(src),
		Recursive: proto.Bool(recursive),
	}
	rsq := &DeleteResponseProto{}

	err := c.rpcProxy.Invoke(delete, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) Mkdirs(src string, masked FsPermissionProto, createParent bool) (bool, error) {
	req := &MkdirsRequestProto{
		Src:          proto.String(src),
		Masked:       &masked,
		CreateParent: proto.Bool(createParent),
	}
	rsq := &MkdirsResponseProto{}

	err := c.rpcProxy.Invoke(mkdirs, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func (c ClientNamenodeProtocolTranslatorPB) RenewLease(clientName string) error {
	req := &RenewLeaseRequestProto{
		ClientName: proto.String(clientName),
	}
	rsq := &RenewLeaseResponseProto{}

	err := c.rpcProxy.Invoke(renewLease, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return nil
}

func (c ClientNamenodeProtocolTranslatorPB) RecoverLease(src, clientName string) (bool, error) {
	req := &RecoverLeaseRequestProto{
		Src:        proto.String(src),
		ClientName: proto.String(clientName),
	}
	rsq := &RecoverLeaseResponseProto{}

	err := c.rpcProxy.Invoke(renewLease, req, rsq)
	if err != nil {

	}
	//todo to remove
	fmt.Println("==================")
	fmt.Println(rsq)
	return rsq.GetResult(), nil
}

func convertCreateFlag(flag []CreateFlagProto) uint32 {
	var value int32
	for _, v := range flag {
		value |= CreateFlagProto_value[v.String()]
	}
	//todo
	return uint32(value)
}
