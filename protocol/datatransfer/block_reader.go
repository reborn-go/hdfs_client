package datatransfer

type BlockReader interface {
	read(buf []byte) (n int, err error)
	skip(n int64) (err error)
}
