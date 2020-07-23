package client

import (
	"fmt"
	"github.com/The-Data-Appeal-Company/s3-gompress/compressors"
	"github.com/The-Data-Appeal-Company/s3-gompress/test"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mitchelldavis/go_localstack/pkg/localstack"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"testing"
)

var LOCALSTACK *localstack.Localstack
var bucketName = "bachettaccio"

type LocalStackTestSuite struct {
	suite.Suite
}

func TestS3PlainClientSuite(t *testing.T) {
	InitializeLocalStack()
	defer LOCALSTACK.Destroy()
	suite.Run(t, new(LocalStackTestSuite))
}

func InitializeLocalStack() {
	s3, _ := localstack.NewLocalstackService("s3")

	// Gather them all up...
	LOCALSTACK_SERVICES := &localstack.LocalstackServiceCollection{
		*s3,
	}

	// Initialize the services
	var err error

	LOCALSTACK, err = localstack.NewLocalstack(LOCALSTACK_SERVICES)
	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to create the localstack instance: %s", err))
	}
	if LOCALSTACK == nil {
		log.Fatal("LOCALSTACK was nil.")
	}
}

func initStack(svc *s3.S3) error {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

func (suite *LocalStackTestSuite) TestShouldGetFromS3() {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)
	if err != nil {
		suite.Fail(err.Error())
	}
	s := NewS3PlainClient(
		sess,
		bucketName,
	)
	want := test.ReadFileOrError(suite.T(), "test_data/input.json")
	_ = s.Put("input.json", want)
	got, err := s.Get("input.json")
	if err != nil {
		suite.Fail(err.Error())
	}
	suite.Equal(got, want)
}

func (suite *LocalStackTestSuite) TestShouldErrorIfBucketNotExistsGet() {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)
	if err != nil {
		suite.Fail(err.Error())
	}
	s := NewS3PlainClient(
		sess,
		"non-esisto",
	)

	_, err = s.Get("input.json")
	suite.Error(err)
}

func (suite *LocalStackTestSuite) TestShouldErrorIfFileNotExists() {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)
	if err != nil {
		suite.Fail(err.Error())
	}
	s := NewS3PlainClient(
		sess,
		"non-esisto",
	)

	_ = s.Put("input.json", test.ReadFileOrError(suite.T(), "test_data/input.json"))
	_, err = s.Get("nullaaa.json")
	suite.Error(err)
}

func (suite *LocalStackTestSuite) TestShouldPutOnS3() {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)

	if err != nil {
		suite.Fail(err.Error())
	}
	s := NewS3PlainClient(
		sess,
		bucketName,
	)
	key := "antani.json"
	obj := test.ReadFileOrError(suite.T(), "test_data/input.json")
	err = s.Put(key, obj)
	suite.NoError(err)

	object, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		suite.Fail(err.Error())
	}
	suite.NotNil(object)
	all, err := ioutil.ReadAll(object.Body)
	suite.NoError(err)
	suite.Equal(obj, all)
}

func (suite *LocalStackTestSuite) TestShouldErrorIfBucketNotExistsPut() {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)

	if err != nil {
		suite.Fail(err.Error())
	}
	s := NewS3PlainClient(
		sess,
		"non-esisto",
	)
	key := "antani.json"
	obj := test.ReadFileOrError(suite.T(), "test_data/input.json")
	err = s.Put(key, obj)
	suite.Error(err)
}

func (suite *LocalStackTestSuite) TestT() {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		panic(err)
	}

	comp := &compressors.GzipCompressor{}
	s3Client := NewS3CompressorClient(sess, "travelappeal-redshift-unload", comp)
	err = s3Client.Put("my/key.gz", []byte("LOL"))
	if err != nil {
		log.Errorf("oh no! %v", err)
	} else {
		log.Info("hooray")
	}
	//no compression client
	s3ClientPlain := NewS3PlainClient(sess, "travelappeal-redshift-unload")
	err = s3ClientPlain.Put("my/key", []byte("LOL"))
	if err != nil {
		log.Errorf("oh no! %v", err)
	} else {
		log.Info("hooray")
	}
}
