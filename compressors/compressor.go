package compressors

type Compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}
