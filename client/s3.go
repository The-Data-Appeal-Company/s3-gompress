package client

type S3Client interface {
	Put(key string, object []byte) error
	Get(key string) ([]byte, error)
}
