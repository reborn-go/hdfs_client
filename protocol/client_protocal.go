package protocol

import (
	. "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
)

type ClientProtocol interface {
	//Get locations of the blocks of the specified file within the specified range.
	//DataNode locations for each block are sorted by the proximity to the client.
	//Return LocatedBlocks which contains file length, blocks and their locations.
	//DataNode locations for each block are sorted by the distance to the client's address.
	//
	//The client will then have to contact one of the indicated DataNodes to obtain the actual data.
	getBlockLocations(src string, offset uint64, length uint64) (GetBlockLocationsResponseProto, error)

	//Get server default values for a number of configuration params.
	getServerDefaults() (FsServerDefaultsProto, error)

	// Create a new file entry in the namespace.
	//
	// This will create an empty file specified by the source path.
	// The path should reflect a full path originated at the root.
	// The name-node does not have a notion of "current" directory for a client.
	//
	// Once created, the file is visible and available for read to other clients.
	// Although, other clients cannot delete re-create or
	// rename it until the file is completed
	// or explicitly as a result of lease expiration.
	//
	// Blocks have a maximum size.  Clients that intend to create
	// multi-block files must also use  addBlock
	create(src string, masked FsPermissionProto, clientName string, flag []CreateFlagProto,
		createParent bool, replication int16,
		blockSize int64, supportedVersions []CryptoProtocolVersionProto) (HdfsFileStatusProto, error)

	//Append to the end of the file.
	append(src string, clientName string, flag []CreateFlagProto) (LastBlockWithStatus, error)

	// Set replication for an existing file.
	//
	// The NameNode sets replication to the new value and returns.
	// The actual block replication is not expected to be performed during
	// this method call. The blocks will be populated or removed in the
	// background as the result of the routine block maintenance procedures.
	setReplication(src string, replication int16) error

	//Set permissions for an existing file/directory.
	setPermission(src string, permission FsPermissionProto) error

	//Set Owner of a path (i.e. a file or a directory).
	setOwner(src string, username string, groupname string)

	//The client can give up on a block by calling abandonBlock().
	//The client can then either obtain a new block, or complete or abandon the
	//file.
	//Any partial writes to the block will be discarded.
	abandonBlock(b ExtendedBlockProto, fileId int64, src string, holder string) error

	//A client that wants to write an additional block to the
	//indicated filename (which must currently be open for writing)
	//should call addBlock().
	//
	//addBlock() allocates a new block and datanodes the block data
	//should be replicated to.
	//
	//addBlock() also commits the previous block by reporting
	//to the name-node the actual generation stamp and the length
	//of the block that the client has transmitted to data-nodes.
	addBlock(src string, clientName string, previous ExtendedBlockProto,
		excludeNodes []DatanodeInfoProto, fileId int64, favoredNodes []string) (LocatedBlockProto, error)

	//Get a datanode for an existing pipeline.
	getAdditionalDatanode(src string, fileId int64, existings []DatanodeInfoProto,
		existingStorageIDs []string, excludes []DatanodeInfoProto,
		numAdditionalNodes int32, clientName string) (LocatedBlockProto, error)

	complete(src string, clientName string, last ExtendedBlockProto, fileId int64) (bool, error)

	//The client wants to report corrupted blocks (blocks with specified
	//locations on datanodes).
	reportBadBlocks(blocks []LocatedBlockProto) error

	///////////////////////////////////////
	// Namespace management
	///////////////////////////////////////

	//Rename an item in the file system namespace.
	rename(src string, dst string) bool

	//Moves blocks from srcs to trg and delete srcs
	concat(src string, srcs []string) error

	//Truncate file src to new size.
	//
	//  Fails if src is a directory.
	//  Fails if src does not exist.
	//  Fails if src is not closed.
	//  Fails if new size is greater than current size.
	//
	//This implementation of truncate is purely a namespace operation if truncate
	//occurs at a block boundary. Requires DataNode block recovery otherwise.
	truncate(src string, newLength int64, clientName string) (bool, error)

	//Delete the given file or directory from the file system.
	//same as delete but provides a way to avoid accidentally
	//deleting non empty directories programmatically.
	delete(src string, recursive bool) (bool, error)

	//Create a directory (or hierarchy of directories) with the given
	//name and permission.
	mkdirs(src string, masked FsPermissionProto, createParent bool) (bool, error)

	//Client programs can cause stateful changes in the NameNode
	//that affect other clients.  A client may obtain a file and
	//neither abandon nor complete it.  A client might hold a series
	//of locks that prevent other clients from proceeding.
	//Clearly, it would be bad if a client held a bunch of locks
	//that it never gave up.  This can happen easily if the client
	//dies unexpectedly.
	//
	//So, the NameNode will revoke the locks and live file-creates
	//for clients that it thinks have died.  A client tells the
	//NameNode that it is still alive by periodically calling
	//renewLease().  If a certain amount of time passes since
	//the last call to renewLease(), the NameNode assumes the
	//client has died.
	renewLease(clientName string) error

	//Start lease recovery.
	//Lightweight NameNode operation to trigger lease recovery
	recoverLease(src, clientName string) (bool, error)

	//Todo
}
