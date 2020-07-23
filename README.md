# s3-gompress
[![Go Report Card](https://goreportcard.com/badge/github.com/The-Data-Appeal-Company/sqs-consumer)](https://goreportcard.com/report/github.com/The-Data-Appeal-Company/s3-gompress)
![Go](https://github.com/The-Data-Appeal-Company/s3-gompress/workflows/Go/badge.svg)
[![license](https://img.shields.io/github/license/The-Data-Appeal-Company/s3-gompress.svg)](LICENSE)

### Simple AWS S3 client with compression/decompression for files
S3-gompressor allows getting and putting objects on S3 without worrying about decompression/compression

### Usage 

```go
package main

import (
    "github.com/The-Data-Appeal-Company/s3-gompress/client"
    "github.com/The-Data-Appeal-Company/s3-gompress/compressors"
    "github.com/aws/aws-sdk-go/aws/session"
    log "github.com/sirupsen/logrus"
)

func main() {
    sess, err := session.NewSessionWithOptions(session.Options{
        SharedConfigState: session.SharedConfigEnable,
    })
    if err != nil {
        panic(err)
    }
    //using gzip as compression/decompression
    comp := &compressors.GzipCompressor{}
    s3Client := client.NewS3CompressorClient(sess, "mybucket", comp)
    err = s3Client.Put("my/key.gz", []byte("LOL"))
    if err != nil {
        log.Errorf("oh no! %v", err)
    } else {
        log.Info("hooray")
    }
    //no compression client
    s3ClientPlain := client.NewS3PlainClient(sess, "mybucket")
    err = s3ClientPlain.Put("my/key", []byte("LOL"))
    if err != nil {
        log.Errorf("oh no! %v", err)
    } else {
        log.Info("hooray")
    }
}
``` 
