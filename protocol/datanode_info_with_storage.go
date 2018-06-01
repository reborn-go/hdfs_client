package protocol

type DatanodeInfoWithStorage struct {
	DatanodeInfo
	StorageID string
}

type DatanodeInfo struct {
	DatanodeID
	Capacity            int64
	DfsUsed             int64
	NonDfsUsed          int64
	Remaining           int64
	BlockPoolUsed       int64
	CacheCapacity       int64
	CacheUsed           int64
	LastUpdate          int64
	LastUpdateMonotonic int64
	XceiverCount        int32
	Location            string
	SoftwareVersion     string
	DependentHostNames  []string
}

type DatanodeID struct {
	IpAddr         string // IP address
	HostName       string // hostname claimed by datanode
	PeerHostName   string // hostname from the actual connection
	XferPort       int32  // data streaming port
	InfoPort       int32  // info server port
	InfoSecurePort int32  // info server port
	IpcPort        int32  // IPC server port
	XferAddr       string
	DatanodeUuid   string
}
