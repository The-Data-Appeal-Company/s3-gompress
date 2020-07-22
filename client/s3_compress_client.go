package client

import (
	"github.com/The-Data-Appeal-Company/s3-gompress/compressors"
	"github.com/aws/aws-sdk-go/aws/client"
)

type S3CompressorClient struct {
	s3Client   S3Client
	compressor compressors.Compressor
}

func NewS3CompressorClient(sess client.ConfigProvider, bucket string, compressor compressors.Compressor) *S3CompressorClient {
	plainClient := NewS3PlainClient(sess, bucket)
	return &S3CompressorClient{
		s3Client:   plainClient,
		compressor: compressor,
	}
}

func (s *S3CompressorClient) Put(key string, object []byte) error {
	compressed, err := s.compressor.Compress(object)
	if err != nil {
		return err
	}
	return s.s3Client.Put(key, compressed)
}

func (s *S3CompressorClient) Get(key string) ([]byte, error) {
	object, err := s.s3Client.Get(key)
	if err != nil {
		return []byte{}, err
	}
	decompressed, err := s.compressor.Decompress(object)
	if err != nil {
		return []byte{}, err
	}
	return decompressed, nil
}
