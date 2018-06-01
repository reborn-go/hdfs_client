package ipc

import "github.com/rs/xid"

func getClientId() []byte {
	guid := xid.New()
	return guid.Bytes()
}
