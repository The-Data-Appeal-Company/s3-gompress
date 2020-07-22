package client

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

type S3PlainClient struct {
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
}

func NewS3PlainClient(sess client.ConfigProvider, bucket string) *S3PlainClient {
	return &S3PlainClient{uploader: s3manager.NewUploader(sess), downloader: s3manager.NewDownloader(sess), bucket: bucket}
}

func (s *S3PlainClient) Put(key string, object []byte) error {
	// Upload the file to S3.
	result, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(object),
	})
	if err != nil {
		log.Errorf("failed to upload file, %v", err)
		return err
	}
	log.Debugf("file uploaded to, %s\n", result.Location)
	return nil
}

func (s *S3PlainClient) Get(key string) ([]byte, error) {
	var buff aws.WriteAtBuffer
	_, err := s.downloader.Download(&buff, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return []byte{}, fmt.Errorf("failed to download file, %v", err)
	}
	return buff.Bytes(), nil
}
